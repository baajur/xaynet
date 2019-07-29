import tensorflow as tf
from absl import logging

from . import config, data, persistence


def generate_cifar10_random_splits_10():
    logging.info("Generate generate_cifar10_random_splits_10")

    dataset = data.generate_splits(
        num_splits=10,
        validation_set_size=5000,
        keras_dataset=tf.keras.datasets.cifar10,
        transformer=data.random_shuffle,
    )

    persistence.save_splits(
        dataset_name="cifar10_random_splits_10",
        dataset=dataset,
        local_generator_dir=config.local_generator_datasets_dir,
    )


def generate_fashion_mnist_10s_600():
    logging.info("Generate fashion_mnist_10s_600")

    dataset = data.generate_splits(
        num_splits=10,
        validation_set_size=6000,
        keras_dataset=tf.keras.datasets.fashion_mnist,
        transformer=data.balanced_labels_shuffle,
    )

    persistence.save_splits(
        dataset_name="fashion_mnist_10s_600",
        dataset=dataset,
        local_generator_dir=config.local_generator_datasets_dir,
    )


def generate_fashion_mnist_10s_500_1k_bias():
    logging.info("Generate fashion_mnist_10s_500_1k_bias")

    dataset = data.generate_splits(
        num_splits=10,
        validation_set_size=6000,
        keras_dataset=tf.keras.datasets.fashion_mnist,
        transformer=data.biased_balanced_labels_shuffle,
    )

    persistence.save_splits(
        dataset_name="fashion_mnist_10s_500_1k_bias",
        dataset=dataset,
        local_generator_dir=config.local_generator_datasets_dir,
    )


def generate_fashion_mnist_10s_single_class():
    logging.info("Generate fashion_mnist_10s_single_class")

    dataset = data.generate_splits(
        num_splits=10,
        validation_set_size=6000,
        keras_dataset=tf.keras.datasets.fashion_mnist,
        transformer=data.group_by_label,
    )

    persistence.save_splits(
        dataset_name="fashion_mnist_10s_single_class",
        dataset=dataset,
        local_generator_dir=config.local_generator_datasets_dir,
    )


def generate_fashion_mnist_100p_IID_balanced():
    logging.info("Generate fashion_mnist_100p_IID")

    dataset = data.generate_splits(
        num_splits=100,
        validation_set_size=6000,
        keras_dataset=tf.keras.datasets.fashion_mnist,
        transformer=data.balanced_labels_shuffle,
    )

    persistence.save_splits(
        dataset_name="fashion_mnist_100p_IID_balanced",
        dataset=dataset,
        local_generator_dir=config.local_generator_datasets_dir,
    )