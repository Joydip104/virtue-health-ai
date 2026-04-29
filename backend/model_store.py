import os
import joblib

MODEL_DIR = "models"
os.makedirs(MODEL_DIR, exist_ok=True)

def model_path(name):
    return os.path.join(MODEL_DIR, name)

def save_model(model, filename):
    path = model_path(filename)
    joblib.dump(model, path)
    return path

def load_model(filename):
    path = model_path(filename)
    return joblib.load(path)