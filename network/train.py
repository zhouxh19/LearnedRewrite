from scale import ScaledDotProductAttention
from encoding import PositionalWiseFeedForward
import math
import json
import numpy as np
import torch
import torch.nn as nn
from torchsummary import summary 
import torch.utils.data as Data
import matplotlib.pyplot as plt
import ipdb
import time

MAX_LEN_PADDING = 5
def padding_to_same(x_list):
    max_len = 0
    for list_item in x_list:
        if len(list_item) > max_len:
            max_len = len(list_item)
    max_len += MAX_LEN_PADDING
    ret_list = []
    for list_item in x_list:
        relative_len = max_len - len(list_item)
        for _ in range(0, relative_len):
            list_item.append(0)
        ret_list.append(list_item)
    return ret_list
    

d = 25
n = 200
x_list = []
y_list = []
with open("../2queries_output_file.txt","r") as f:
    for cnt, line in enumerate(f):
        if cnt % 3 == 1:
            x_list.append(eval(line[:-1]))
        elif cnt % 3 == 2:
            tmp_num = eval(line[:-1])
            if tmp_num > 0:
                tmp_num = math.log(tmp_num)
            else:
                tmp_num = 0.0
            y_list.append(tmp_num)

x_list = padding_to_same(x_list)
x_numpy = np.array(x_list[12:], dtype=float)
y_numpy = np.array(y_list[12:], dtype=float)

BATCH_SIZE = 1
X = torch.from_numpy(x_numpy)  
y = torch.from_numpy(y_numpy)
torch_dataset = Data.TensorDataset(X, y)
loader = Data.DataLoader(
    dataset=torch_dataset,      # 数据，封装进Data.TensorDataset()类的数据
    batch_size=BATCH_SIZE,      # 每块的大小
    shuffle=True,               # 要不要打乱数据 (打乱比较好)
    num_workers=4,              # 多进程（multiprocess）来读数据
)


#注意这里hid_dim 设置是超参数(如果太小，效果就不好)，使用tanh还是relu效果也不同，优化器自选
hid_dim_1 = 100
hid_dim_2 = 32
hid_dim_3 = 64
d_out = 4

TIME_STEP = 10
INPUT_SIZE = 1
LR = 0.01
EPOCHS = 500
DEFINE_MODEL_DIM = 5
 
class Rnn(nn.Module):
    def __init__(self, in_dim, hidden_dim, n_layer, n_classes):
        super(Rnn, self).__init__()
        self.n_layer = n_layer
        self.hidden_dim = hidden_dim
        self.lstm = nn.LSTM(in_dim, hidden_dim, n_layer, batch_first=True)
        self.classifier = nn.Linear(hidden_dim, n_classes)

    def forward(self, x):
        out, (h_n, c_n) = self.lstm(x)
        # 此时可以从out中获得最终输出的状态h
        # x = out[:, -1, :]
        x = h_n[-1, :, :]
        x = self.classifier(x)
        return x

# 选择模型
model = Rnn(in_dim=25, hidden_dim=hid_dim_1, n_layer=2, n_classes=1)
        #nn.Sequential(
       
#        nn.Flatten(),
#        nn.Linear(hid_dim_1, hid_dim_2),
#        nn.Dropout(0.2),
#        nn.Linear(hid_dim_2, d_out)
#        )
# model.load_state_dict(torch.load("../model/1.pth"))
# summary(model, input_size=(4, 25, 4))
 
# 定义优化器和损失函数
loss_func = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=LR)
 
h_state = None # 第一次的时候，暂存为0
 
loss_list = []
print("epoch\t loss\t")
for step in range(EPOCHS):
    loss = 0.0
    for X, y in loader:   
        if X.size()[0] == 1: # 去除最后一个数据 & 格式检查
            # print(X.T.reshape(1, 1, 25).to(torch.float32).size())
            # time.sleep(15)
            prediction = model(X.T.reshape(1, 1, 25).to(torch.float32))
            # time.sleep(2)
            loss = loss_func(prediction.reshape(1).float(), y.reshape(1).float())
            # print(loss)
            optimizer.zero_grad()
            loss.to(torch.float32)
            loss.backward()
            optimizer.step()
    loss_list.append(loss)
    print("\t {:.5f}".format(loss))

torch.save(model.state_dict(), "../model/1.pth")
x_axis = [i for i in range(0, len(loss_list))]
plt.plot(x_axis, loss_list, 'r-')
plt.savefig("./loss_item.png")

test_x_numpy = np.array(x_list[:16])
test_y_numpy = np.array(y_list[:16])
test_x_tensor = torch.Tensor(test_x_numpy)
test_y_tensor = torch.Tensor(test_y_numpy)
print(test_x_tensor.size())
predict_y_tensor = model(test_x_tensor, h_state)
print( str(loss_func(predict_y_tensor, test_y_tensor)) )
