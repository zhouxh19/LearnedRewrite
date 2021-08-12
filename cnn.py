import numpy as np
import torch
from torch import nn
import torch.utils.data as Data
import matplotlib.pyplot as plt
from torch.optim import lr_scheduler
import torch.nn.functional as F
import ipdb
import math

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
with open("./2queries_output_file.txt","r") as f:
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
x_numpy = np.array(x_list[12:])
y_numpy = np.array(y_list[12:])

BATCH_SIZE = 32
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
hid_dim_1 = 128
hid_dim_2 = 64
hid_dim_3 = 32
d_out = 1

class CNN(nn.Module):
    def __init__(self):
        super(CNN, self).__init__()
        
        Dim = 25 ## 每个词向量长度
        Cla = 1 ## 类别数
        Ci = 1 ##输入的channel数
        Knum = 2 ## 每种卷积核的数量
        Ks = [2, 3, 4] ## 卷积核list，形如[2,3,4]
        
        self.convs = nn.ModuleList([nn.Conv2d(Ci, Knum, (K, Dim)) for K in Ks]) ## 卷积层
        self.dropout = nn.Dropout(0.2) 
        self.fc = nn.Linear(len(Ks)*Knum, Cla) ##全连接层
        
    def forward(self,x):
        
        # x = x.unsqueeze(1) #(N,Ci,W,D)
        x = [F.relu(conv(x)).squeeze(3) for conv in self.convs] # len(Ks)*(N,Knum,W)
        x = [F.max_pool1d(line,line.size(2)).squeeze(2) for line in x]  # len(Ks)*(N,Knum)
        
        x = torch.cat(x,1) #(N,Knum*len(Ks))
        
        x = self.dropout(x)
        logit = self.fc(x)
        return logit

model = CNN()
# model.cuda()
loss_func = nn.MSELoss()
optim = torch.optim.SGD(model.parameters(), 0.05, momentum = 0.8)
scheduler = lr_scheduler.StepLR(optim, step_size=100, gamma=0.1)

epochs = 1000000
print("epoch\t loss\t")
for i in range(epochs):
    for X, y in loader:
        X = X.to(torch.float32)
        y_hat = model(X.reshape(4, 1, 8, 25))
        # y_hat
        # print(y_hat.size())
        # print(y.float)
        y_hat = y_hat.reshape(16)
        loss = loss_func(y_hat, y.float())
        # ipdb.set_trace()
        optim.zero_grad()
        loss.backward()
        # ipdb.set_trace()
        optim.step()
        scheduler.step()    
        # print(loss)
    print("\t {:.5f}".format(loss.item()))

test_x_numpy = np.array(x_list[:16])
test_y_numpy = np.array(y_list[:16])
test_x_tensor = torch.Tensor(test_x_numpy)
test_y_tensor = torch.Tensor(test_y_numpy)
predict_y_tensor = model(test_x_tensor)
print( str(loss_func(predict_y_tensor, test_y_tensor)) )
