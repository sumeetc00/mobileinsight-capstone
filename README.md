# Edgeless Distributed Computing Platform for Mobile Analytics

This is the capstone project for Sumeet Chaudhari's UCLA MS CS program. I have developed a mobile Spark analytics program to understand and diagnose performance of mobile devices in cellular access networks (4G and 5G) in an edgeless platform. These analytics tools in an edgeless, distributed platform will enable real-time insights into under performing applications on such mobile devices.

## Requirements

In order to run the Spark analytic program, mobile devices must have:

- Linux Deploy ([installation tutorial](https://github.com/meefik/linuxdeploy))
 - Apache Spark ([installation tutorial](https://phoenixnap.com/kb/install-spark-on-ubuntu))
 - MobileInsight ([installation tutorial](https://github.com/mobile-insight/mobileinsight-core))
 
Note: MobileInsight requires Wireshark v3.4 which will automatically be installed using the provided installation scripts (`install-ubuntu.sh`)

## Setup

 1. use Linux Deploy to install a Ubuntu OS on the mobile device with disk image size of **at least 8 GB**
 2.  use the tutorials above to install Apache Spark and MobileInsight on Ubuntu OS
 3. clone this repository somewhere within the Ubuntu OS (EXCEPT on the mounted sd card)
 4. replace the `mobile_insight/monitor/offline_replayer.py` file from the installed mobile_insight library to the `offline_replayer.py` file in this repository
 
 ## How to Use

Run the `streaming.py` file from this cloned repository location on Ubuntu while simultaneously running the NetLogger plugin in the MobileInsight app on the mobile device.

After doing so, MobileInsight will start capturing mi2logs which will be stored in the `/sdcard/mobileinsight/logs` directory which our Spark analytics program will read from. 

Resulting graphs and plots are stored in the `/sdcard/plots` directory.
