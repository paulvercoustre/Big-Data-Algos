#! /usr/bin/env python
"""
Extract features from images passed as argument and store the features in the "features" folder.

The features we use are the output values from the fc2 layer of a convolutional
neural net trained on ImageNet (a large image dataset that was built for
research purposes http://www.image-net.org/).

Usage:

    python image_features.py ../data/oxford-IIIT-pet-dataset/images/*.jpg

Note that you will need the following dependencies to run this script:

    # You probably want to do this in a virtualenv
    pip install keras tensorflow h5py pillow numpy
"""
import sys
import os

from keras.applications.vgg16 import VGG16
from keras.models import Model
from keras.preprocessing import image
from keras.applications.vgg16 import preprocess_input
import numpy as np


def main():
    # Load Keras model (this may take some time to download in the first run)
    base_model = VGG16(weights='imagenet')
    model = Model(input=base_model.input, output=base_model.get_layer('fc2').output)

    for image_path in sys.argv[1:]:
        print("Processing {}...".format(image_path))
        
        features_path = image_path + ".npy"

        # Extract features from the image
        img = image.load_img(image_path, target_size=(224, 224))
        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        x = preprocess_input(x)
        features = model.predict(x)

        # Save result in features folder
        np.save(os.path.join(os.path.dirname(__file__), "..", "data", \
            "oxford-IIIT-pet-dataset","features", os.path.split(features_path)[1]), features[0])

if __name__ == "__main__":
    main()
