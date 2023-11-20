from TikTokDataLoader import TikTokDataLoader
# 导入必要的库
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score, classification_report
from sklearn.preprocessing import StandardScaler
def train_svm(data):
    #Keep only numerical data
    data=data.select_dtypes(include=np.number)
    #Split labeling and features
    X = data.drop('is_bot', axis=1)
    y = data['is_bot']
    #Split training/testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    #Standardize features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    #Set SVM model
    svm_model = SVC(kernel='linear', C=1)
    svm_model.fit(X_train_scaled, y_train)
    decision_values = svm_model.decision_function(X_test_scaled)
    threshold = 0.0
    y_pred = (decision_values > threshold).astype(int)
    accuracy = accuracy_score(y_test, y_pred)
    print(f'Accuracy: {accuracy:.2f}')
    print(classification_report(y_test, y_pred))
    return svm_model
def predict(data,model,threshold):
    #Use model to predict the bot follower percentage of an account
    scaler = StandardScaler()
    data = data.select_dtypes(include=np.number)
    X_scaled = scaler.fit_transform(data)
    decision_values = model.decision_function(X_scaled)
    average_prediction = (decision_values > threshold).astype(int).mean()
    print('Avg of Predicted y= '+str(average_prediction))
    return average_prediction