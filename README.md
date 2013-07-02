hadoop-apps
===========

A repository to host experimental MapReduce apps developed at UMD Libraries.

### Hadoop Thumbnail Generator
A simple app that generates thumbnails of images. This app reads the input images from HDFS, and writes the generated thumbnails back to HDFS. [ImgScalr](https://github.com/thebuzzmedia/imgscalr) library is used for resizing the image.

### FileAnalyzer
This app helps compare directories to identify missing and/or corrupted files.
