import torch, math
import pandas as pd
import numpy as np
import torch.utils.data as Data
import matplotlib.pyplot as plt
from torch import nn, optim
from torch.nn import functional as func
from sklearn.model_selection import train_test_split
from torch.autograd import Variable

def embedding(algo, time):
    temp_algo = []
    temp_time = []
    for j in range(len(algo)):
        if(time[j] < 1e-6):
            continue
        temp_algo.append(diction[algo[j]])
        temp_time.append(math.log(time[j]))
    
    algo = func.one_hot(torch.tensor(temp_algo), num_classes = num + 1)
    output = torch.unsqueeze(torch.tensor(temp_time), dim = 1)
    
    temp = torch.cat((algo, output), dim = 1)
    temp = torch.cat((temp, torch.zeros(maxlen - temp.shape[0], temp.shape[1])), dim = 0)
    return torch.unsqueeze(temp.reshape(1, -1), dim = 0)

diction = dict()
num = -1
maxlen = 0

def read_file(filename):
    global diction, num, maxlen
    input_file = open(filename, "r")
    input_string = input_file.readlines()
    total = len(input_string) // 3

    X_all = torch.tensor([])
    y_all = []

    for i in range(total):
        y_all.append(float(input_string[3 * i + 2]))
        algo = eval(input_string[3 * i])
        if len(algo) > maxlen:
            maxlen = len(algo)
        for word in algo:
            if word not in diction:
                num += 1
            diction[word] = num
    y_all = torch.log(torch.tensor(y_all))
    y_all[torch.isinf(y_all)] = 0

    for i in range(total):
        algo = eval(input_string[3 * i])
        time = eval(input_string[3 * i + 1])
        X_all = torch.cat((X_all, embedding(algo, time)), dim = 0)

    y_all = torch.unsqueeze(y_all, dim = 1)
    X_all = torch.squeeze(X_all)

    return X_all, y_all, total

class MLP(nn.Module):

    def __init__(self, layers, input_dim, hidden_dim):
        super(MLP, self).__init__()
        self.layers = layers
        self.linear1 = nn.Linear(input_dim, hidden_dim)
        self.linear2 = nn.Linear(hidden_dim, hidden_dim)
        self.linear3 = nn.Linear(hidden_dim, 1)

    def forward(self, inputs):
        model = self.linear1(inputs)
        model = func.relu(model)
        for i in range(1, self.layers):
            model = self.linear2(model)
            model = func.leaky_relu(model)
            model = self.linear2(model)
            model = func.relu(model)
        model = func.dropout(model, p = 0.5)
        model = self.linear3(model)
        model = func.tanh(model) * 70
        return model
        
def solve():
    X_all, y_all, total = read_file("../input_length.txt")

    X_all = Variable(X_all)
    y_all = Variable(y_all)

    model = MLP(layers = 14, input_dim = X_all.shape[1], hidden_dim = 256 * 3)
    # model.cuda(0)
    optimizer = torch.optim.Adam(model.parameters(), lr = 0.00005)

    BATCH_SIZE = 1024
    torch_dataset = Data.TensorDataset(X_all, y_all)
    loader = Data.DataLoader(
        dataset = torch_dataset,
        batch_size = BATCH_SIZE,
        shuffle = True,         
        num_workers = 0,        
    )

    criterion = nn.MSELoss()
    loss_list = []
    epochs = 3000
#    torch.set_printoptions(profile = "full")
    for i in range(epochs):
        for X, y in loader:
            y_hat = model(X)
            loss = criterion(y_hat, y.float())
            loss.backward()
            optimizer.step()
            nn.utils.clip_grad_norm_(model.parameters(), max_norm = 3)
        loss_list.append(loss)
        print("\tcases:", i, "Loss: {:.5f}".format(loss))

    #torch.save(model.state_dict(), "./model/1.pth")
    x_axis = [i for i in range(0, len(loss_list))]
    plt.plot(x_axis, loss_list, 'r-')
    plt.savefig("./loss.png")

    return model

def predict(filename, model):
    X_all, y_all, total = read_file(filename)
    X_all = Variable(X_all)
    y_all = Variable(y_all)

    predict_y = model(X_all)
    predDF = pd.DataFrame(
        {'normal': torch.exp(y_all.cpu()).tolist(), 
        'predict': torch.exp(predict_y.cpu()).tolist()})
    predDF.to_csv('result.csv', index = False)

model = solve()
predict("../2.txt", model)
