# Tesseract language data

`ara.traineddata` is copied unchanged from [tesseract-ocr/tessdata_fast](https://github.com/tesseract-ocr/tessdata_fast) at tag `4.1.0` (Apache License 2.0). `SHA256SUMS` holds its checksum.

The official Paperless-ngx image ships with deu, eng, fra, ita and spa. Tenants sit on an internal network and can't install packages when they start, so compose mounts this file read-only into `/usr/share/tesseract-ocr/5/tessdata/`. The image itself is not modified.
