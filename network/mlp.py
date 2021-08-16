import numpy as np
import torch
from torch import nn
import torch.utils.data as Data
import matplotlib.pyplot as plt
from torch.optim import lr_scheduler
import ipdb

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
            tmp_list = []
            for _ in range(0, 16):
                tmp_list.append(tmp_num % 10)
                tmp_num /= 10
            y_list.append(tmp_list)

x_list = padding_to_same(x_list)
x_numpy = np.array(x_list[12:], dtype=float)
y_numpy = np.array(y_list[12:], dtype=float)

BATCH_SIZE = 16
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
d_out = 16

model = nn.Sequential(nn.Linear(d,hid_dim_1),
                     nn.Tanh(),
                     nn.Linear(hid_dim_1, hid_dim_2),
                     nn.Tanh(),
                     nn.Linear(hid_dim_2, hid_dim_3),
                     nn.Tanh(),
                     nn.Linear(hid_dim_3, d_out)
                     )
# model.cuda()
loss_func = nn.MSELoss()
optim = torch.optim.SGD(model.parameters(), 0.3)
scheduler = lr_scheduler.StepLR(optim, step_size=7, gamma=0.1)

epochs = 6000
print("epoch\t loss\t")
for i in range(epochs):
    for X, y in loader:
        # print(X.size(), y.size())
        y_hat = model(X.float())
        # y_hat
        # print(y_hat.size())
        # print(y.float)
        y_hat = y_hat.reshape(16, 16)
        loss = loss_func(y_hat, y.float())
        # ipdb.set_trace()
        optim.zero_grad()
        loss.backward()
        optim.step()
        scheduler.step()    
    print("\t {:.5f}".format(loss.item()))

test_x_numpy = np.array(x_list[:16])
test_y_numpy = np.array(y_list[:16])
test_x_tensor = torch.Tensor(test_x_numpy)
test_y_tensor = torch.Tensor(test_y_numpy)
predict_y_tensor = model(test_x_tensor)
print( str(loss_func(predict_y_tensor, test_y_tensor)) )
