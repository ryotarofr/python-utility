"""
Install

pip install Pillow
"""

from PIL import Image

# TIFF画像のパス
tiff_path = "./test.tif"

# WebP画像のパス
webp_path = "output_image.webp"

# TIFF画像を開く
tiff_image = Image.open(tiff_path)

# WebP形式で保存する
tiff_image.save(webp_path, "WEBP")

print("Conversion complete.")
