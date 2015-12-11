# cs7210-project

Final project for CS 7210 Fall 2015. This project is a demonstration of distributed machine learning using streaming data. We used Storm and Trident to handle the distributed aspect of the project and Trident-ML to do the machine learning. We also included a library for spike detection that detects spikes in a moving window over the data stream, which we adapted from here: https://github.com/stormprocessor/storm-examples. Our streaming data source was an Arduino hooked up to a light detector that passed in values corresponding to the intensity it detected.

You can set up this project on Eclipse. Maven will take care of all the dependencies. If you don't have an Arduino on hand, you can change references to LightDetectionSpout.java to InputStreamSpout.java. Other changes you will need to make are indicated in comments located at the site of any errors that will arise from this change.

InputStreamSpout simulates a data stream akin to that which the Arduino light detector provides, complete with spikes. You can configure the way this stream is produces as well as its duration to your heart's content.
