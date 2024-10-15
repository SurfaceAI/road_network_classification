import os
import sys
# from torch.utils.data import Subset
from functools import partial
from pathlib import Path

import torch
from torch import Tensor, nn
from torchvision import models, transforms

# local modules
src_dir = Path(os.path.abspath(__file__)).parent.parent
sys.path.append(str(src_dir))

import constants as const


class ModelInterface:

    def __init__(self, config):
        # TODO: doc string
        self.device = torch.device(
            f"cuda:{config.get('gpu_kernel')}" if torch.cuda.is_available() else "cpu"
        )
        normalization = (const.NORM_MEAN, const.NORM_SD)
        # TODO: config is changed by transform['normalize'] = normalization
        transform = config.get('transform_surface')
        transform['normalize'] = normalization
        self.transform_surface = transform
        transform = config.get('transform_road_type')
        transform['normalize'] = normalization
        self.transform_road_type = transform 
        self.model_root = config.get('model_root')
        self.models = config.get('models')
        self.batch_size = config.get('batch_size')

    @staticmethod
    def custom_crop(img, crop_style=None):
        im_width, im_height = img.size
        if crop_style == const.CROP_LOWER_MIDDLE_THIRD:
            top = im_height / 3 * 2
            left = im_width / 3
            height = im_height - top
            width = im_width / 3
        elif crop_style == const.CROP_LOWER_MIDDLE_HALF:
            top = im_height / 2
            left = im_width / 4
            height = im_height / 2
            width = im_width / 2
        elif crop_style == const.CROP_LOWER_HALF:
            top = im_height / 2
            left = 0
            height = im_height / 2
            width = im_width
        else:  # None, or not valid
            return img

        cropped_img = transforms.functional.crop(img, top, left, height, width)
        return cropped_img


    def transform(
        self,
        resize=None,
        crop=None,
        to_tensor=True,
        normalize=None,
    ):
        """
        Create a PyTorch image transformation function based on specified parameters.

        Parameters:
            - resize (tuple or None): Target size for resizing, e.g. (height, width).
            - crop (string): crop style e.g. 'lower_middle_third'
            - to_tensor (bool): Converts the PIL Image (H x W x C) in the range [0, 255] to a torch.FloatTensor of shape (C x H x W) in the range [0.0, 1.0]
            - normalize (tuple of lists [r, g, b] or None): Mean and standard deviation for normalization.

        Returns:
            PyTorch image transformation function.
        """
        transform_list = []

        if crop is not None:
            transform_list.append(transforms.Lambda(partial(self.custom_crop, crop_style=crop)))

        if resize is not None:
            if isinstance(resize, int):
                resize = (resize, resize)
            transform_list.append(transforms.Resize(resize))

        if to_tensor:
            transform_list.append(transforms.ToTensor())

        if normalize is not None:
            transform_list.append(transforms.Normalize(*normalize))

        composed_transform = transforms.Compose(transform_list)
        return composed_transform

    def preprocessing(self, img_data_raw, transform):
        transform = self.transform(**transform)
        img_data = torch.stack([transform(img) for img in  img_data_raw])
        return img_data
    
    def load_model(self, model):
        model_path = Path(self.model_root) / model
        model_state = torch.load(model_path, map_location=self.device)
        model_cls = model_mapping[model_state['config']['model']]
        is_regression = model_state['config']["is_regression"]
        valid_dataset = model_state['dataset']

        if is_regression:
            class_to_idx = valid_dataset.get("class_to_idx")
            classes = {str(i): cls for cls, i in class_to_idx.items()}
            num_classes = 1
        else:
            classes = valid_dataset.get("classes")
            num_classes = len(classes)
        model = model_cls(num_classes)
        model.load_state_dict(model_state['model_state_dict'])

        return model, classes, is_regression

    def predict(self, model, data, is_regression, classes):
        model.to(self.device)
        model.eval()

        image_batch = data.to(self.device)

        with torch.no_grad():
            
            batch_outputs = model(image_batch)
            if is_regression:
                batch_outputs = batch_outputs.flatten()
                batch_classes = ["outside" if str(pred.item()) not in classes.keys() else classes[str(pred.item())] for pred in batch_outputs.round().int()]
                batch_values = [pred.item() for pred in batch_outputs]
            else:
                batch_outputs = model.get_class_probabilies(batch_outputs)
                batch_classes = [classes[idx.item()] for idx in torch.argmax(batch_outputs, dim=1)]
                batch_values = [pred.item() for pred in batch_outputs.max(dim=1).values]


        return batch_classes, batch_values      


    def batch_classifications(self, img_data_raw):
        # road type
        model, classes, is_regression = self.load_model(model=self.models.get('road_type'))
        data = self.preprocessing(img_data_raw, self.transform_road_type)
        road_pred_classes, road_pred_values = self.predict(model, data, is_regression, classes)
        road_pred_values = [round(value, 5) for value in road_pred_values]

        # surface type
        model, classes, is_regression = self.load_model(model=self.models.get('surface_type'))
        data = self.preprocessing(img_data_raw, self.transform_surface)
        surface_pred_classes, surface_pred_values = self.predict(model, data, is_regression, classes)
        surface_pred_values = [round(value, 5) for value in surface_pred_values]


        # surface quality
        sub_models = self.models.get('surface_quality')

        surface_indices = {}
        for i, surface_type in enumerate(surface_pred_classes):
            if surface_type not in surface_indices:
                surface_indices[surface_type] = []
            surface_indices[surface_type].append(i)

        quality_pred_values = [None] * len(img_data_raw)
        for surface_type, indices in surface_indices.items():
            sub_model = sub_models.get(surface_type)
            if sub_model is not None:
                model, classes, is_regression = self.load_model(model=sub_model)
                sub_data = data[indices]
                _, pred_values = self.predict(model, sub_data, is_regression, classes)
                pred_values = [round(value, 5) for value in pred_values]

                for i, idx in enumerate(indices):
                    quality_pred_values[idx] = pred_values[i]

        # final results combination
        final_results = []
        for i in range(len(img_data_raw)):
            road = road_pred_classes[i]
            road_prob = road_pred_values[i]
            surface = surface_pred_classes[i]
            surface_prob = surface_pred_values[i]
            quality_value = quality_pred_values[i]

            final_results.append([road, road_prob, surface, surface_prob, quality_value])
        
        return final_results


class CustomEfficientNetV2SLinear(nn.Module):
    def __init__(self, num_classes, avg_pool=1):
        super(CustomEfficientNetV2SLinear, self).__init__()

        model = models.efficientnet_v2_s(weights='IMAGENET1K_V1')
        # adapt output layer
        in_features = model.classifier[-1].in_features * (avg_pool * avg_pool)
        fc = nn.Linear(in_features, num_classes, bias=True)
        model.classifier[-1] = fc
        
        self.features = model.features
        self.avgpool = nn.AdaptiveAvgPool2d(avg_pool)
        self.classifier = model.classifier
        if num_classes == 1:
            self.criterion = nn.MSELoss
        else:
            self.criterion = nn.CrossEntropyLoss
        
    @ staticmethod
    def get_class_probabilies(x):
        return nn.functional.softmax(x, dim=1)

    def forward(self, x: Tensor) -> Tensor:
        x = self.features(x)

        x = self.avgpool(x)
        x = torch.flatten(x, 1)

        x = self.classifier(x)

        return x
    
    def get_optimizer_layers(self):
        return self.classifier

model_mapping = {
    const.EFFNET_LINEAR: CustomEfficientNetV2SLinear,
}
