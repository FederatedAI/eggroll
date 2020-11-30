[<img src="logo.png" align="center" alt="drawing" width="800">](https://github.com/WeBankFinTech/eggroll) 

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![CodeStyle](https://img.shields.io/badge/Check%20Style-Google-brightgreen)](https://checkstyle.sourceforge.io/google_style.html) [![Pinpoint Satellite](https://img.shields.io/endpoint?url=https%3A%2F%2Fscan.sbrella.com%2Fadmin%2Fapi%2Fv1%2Fpinpoint%2Fshield%2FWeBankFinTech%2Feggroll)](https://github.com/mmyjona/FATE-Serving/pulls) [![Style](https://img.shields.io/badge/Check%20Style-Black-black)](https://checkstyle.sourceforge.io/google_style.html)  


A Simple High Performance Compututing Framework for \[Federated\] Machine Learning


Building and Deploying Eggroll
---
You can check the deploy document here:

[English](https://github.com/WeBankFinTech/eggroll/blob/v2.x/deploy/Eggroll%20Deployment%20Guide.md) [简体中文](https://github.com/WeBankFinTech/eggroll/blob/v2.x/deploy/Eggroll%E9%83%A8%E7%BD%B2%E6%96%87%E6%A1%A3%E8%AF%B4%E6%98%8E.md)



Running Tests
---
Testing requires Eggroll having been built and deployed. Once it is completed, you can try the example test cases:
```bash

# standalone mode
python -m unittest eggroll.roll_pair.test.test_roll_pair.TestRollPairStandalone

# cluster mode
python -m unittest eggroll.roll_pair.test.test_roll_pair.TestRollPairCluster

``` 


Special thanks to:

[<img src="https://www.ej-technologies.com/images/product_banners/jprofiler_small.png">](https://www.ej-technologies.com/products/jprofiler/overview.html)
