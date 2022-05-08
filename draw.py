from matplotlib import pyplot as plt
import matplotlib as mpl


mpl.rcParams['font.family']='SimHei'
mpl.rcParams['font.sans-serif']=['SimHei']
mpl.rcParams['axes.unicode_minus']=False     # 正常显示负号


path_base = './test_res/'
def getData(path):
    length = []
    put = []
    get = []
    dele = []
    with open(path_base+path,'r') as f:
        i = 0
        while line:=f.readline():
            data = line.split(':')[1].split()[0]
            if i == 0:
                length.append(int(data))
            elif i == 1:
                put.append(float(data))
            elif i==2:
                get.append(float(data))
            elif i==3:
                dele.append(float(data))
            i = (i+1)%4
    return length,put,get,dele


def drawL(length,put,get,dele):
    plt.plot(length,put,label='put',marker='.')
    plt.plot(length,get,label='get',marker='.')
    plt.plot(length,dele,label='dele',marker='.')
    plt.xlim(min(length),max(length))
    plt.ylim(0,max(max(put),max(get),max(dele)))
    plt.xlabel('键值对数量/个')
    plt.ylabel('延迟/us')

    plt.grid()
    plt.legend()
    plt.show()

def drawT(length,put,get,dele):
    fig,ax = plt.subplots()
    ax.plot(length,put,label='put',marker='.')
    ax.plot(length,get,label='get',marker='.')
    ax.plot(length,dele,label='dele',marker='.')
    ax.ticklabel_format(style='plain')
    ax.set_xlabel('键值对数量/个')
    ax.set_ylabel('吞吐量/tps')
    #ax.title.set_text('三种操作吞吐量与数据量的关系')
    ax.set_xlim(min(length),max(length))
    ax.grid()
    ax.legend()
    plt.show()
    
length,put,get,dele = getData('test_base_small.txt')

drawL(length,put,get,dele)
#drawT(length,[1e6/i for i in put],[1e6/i for i in get],[1e6/i for i in dele])
