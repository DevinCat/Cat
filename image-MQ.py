import requests ,os
import pika

#这个队列消费来自resolve中的url链接
credentials = pika.PlainCredentials('devinCat', 'devinCat')
#链接rabbit服务器（localhost是本机，如果是其他服务器请修改为ip地址）
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.161.60',5672,'baihe',credentials))
channel = connection.channel()
# 定义exchange和类型
channel.queue_declare(queue='imageChannel')

def callback(ch, method, properties, body):
    print(" [x] %r:%r" % ('imageChannel', body))
    if body:
        #这里需要注意，在前面传过来的url链接此时已经转换为bytrs_like，打印的时候链接前面会出来一个b,
        #所以这里需要进行类型转换，这个小错排了好久。。。^_^要不怎么叫菜鸟呢
        body_str=str(body,encoding="utf_8")#按指定的字符集进行类型转换
        id=str(body_str).split("-")[1]
        img_url=str(body_str).split("-")[0]
        print( type(img_url))
        print(img_url)
        #创建文件夹
        os.makedirs(os.path.join("D:/baihe/", str(id)))
        #文件存入的地址，我是用的用户id，后期使用时也方便，直接用id获取
        path = "D:/baihe/" + str(id ) + "/"
        with open(path + str(id) + ".jpg", 'wb') as f:
            try:
                f.write(requests.get(img_url).content)
            except:
                print(str(id) + "图片请求异常")
            print(str(id ) + "图片存入成功")

# 接收消息并消费
channel.basic_consume(callback,
                      queue='imageChannel',
                      no_ack=True)
 # no_ack=True表示在回调函数中不需要发送确认标识
print(' [*] Waiting for logs. To exit press CTRL+C')
channel.start_consuming()


