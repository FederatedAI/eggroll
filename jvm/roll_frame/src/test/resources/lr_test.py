#  Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import torch
#import pandas as pd
from torch.autograd import Variable

import numpy as np
import torch
import torch.nn.functional as F

torch.manual_seed(2)


x = np.random.randn(10000, 100)
y = np.random.randint(2, size=(10000,1))


#x_data = Variable(torch.Tensor(x).cuda())
x_data = torch.Tensor(x)
#y_data = Variable(torch.Tensor(y).cuda())
y_data = torch.Tensor(y)

class Model(torch.nn.Module):
    def __init__(self):
        super(Model, self).__init__()
        #self.linear = torch.nn.Linear(100, 1).cuda() # 2 in and 1 out
        self.linear = torch.nn.Linear(100, 1)

    def forward(self, x):
        #y_pred = self.linear(x)
        #y_pred = torch.sigmoid(self.linear(x))
        return self.linear(x)

# Our model
model = Model()

def closure():
    predicted =  model(x_data)
    loss = criterion(predicted, y_data)
    optimizer.zero_grad()
    loss.backward()
    return loss


#criterion = torch.nn.BCELoss(size_average=True)
criterion = torch.nn.BCEWithLogitsLoss(size_average=True)
#optimizer = torch.optim.SGD(model.parameters(), lr=0.001)
optimizer = torch.optim.LBFGS(model.parameters(), lr=0.01)
import time

time_start=time.time()
# Training loop
for epoch in range(100):
    # Forward pass: Compute predicted y by passing x to the model
    # Zero gradients, perform a backward pass, and update the weights.
    optimizer.step(closure)

time_end=time.time()
print('totally cost',time_end-time_start)


for f in model.parameters():
    print('data is')
    print(f.data)
    print(f.grad)

w = list(model.parameters())
predictions = torch.sigmoid(model(x_data))


p = predictions*(1-predictions)
h = x_data*p
hesson = torch.mm(torch.transpose(h, 1, 0), h)
inv_hesson = torch.inverse(hesson)
print("hesson shape", hesson.shape)

w0 = w[0].data.numpy()
w1 = w[1].data.numpy()

print(w0, w1)