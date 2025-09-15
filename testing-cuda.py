import torch, tensorflow as tf, xgboost

print("Torch CUDA available:", torch.cuda.is_available())
print("TensorFlow GPUs:", tf.config.list_physical_devices('GPU'))