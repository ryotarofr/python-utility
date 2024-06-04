"""
Install

$ pip install opencv-python pyzbar
"""

import cv2

def read_qr_code_with_opencv(image_path):
    # 画像を読み込む
    image = cv2.imread(image_path)
    
    # QRCodeDetectorを初期化
    detector = cv2.QRCodeDetector()
    
    # QRコードをデコードする
    data, bbox, _ = detector.detectAndDecode(image)
    
    if data:
        print("QR Code Data:", data)
    else:
        print("No QR code found.")

# QRコードを含む画像ファイルのパスを指定する
image_path = './test.png'
read_qr_code_with_opencv(image_path)
