import pika #这是操作MQ的包
from bs4 import BeautifulSoup
import requests,os
import pymysql.cursors #这是操作数据酷的包

#获取数据库的链接
#这里注意，数据库的链接和MQ的链接的变量名字不能相同
connection = pymysql.connect(host='localhost',
      user='root',
      password='root',
      db='project',
      port=3306,
      charset='utf8')
#定义MQ
credentials = pika.PlainCredentials('devinCat', 'devinCat')
# 连接到rabbitmq服务器
connectionMQ = pika.BlockingConnection(pika.ConnectionParameters('192.168.161.60',5672,'baihe',credentials))
channel = connectionMQ.channel()

# 声明消息队列，消息将在这个队列中进行传递。如果队列不存在，则创建
channel.queue_declare(queue='hello_baihe')

# 定义一个回调函数来处理，这边的回调函数就是将信息打印出来。
title=""
#在回调函数中解析从消息对立中获取的body，body的装的是页面请求后获得的对象，MQ通过bytes方式传送，过来之后可以直接使用，无需再转换
def callback(ch, method, properties, body):
    #print(" [x] Received %r" % body)
    soup = BeautifulSoup(body, "html.parser")  # 用"美丽的汤"这个包把请求导的数据转换为可以处理的对象
    #print(soup)

    # --------------------------抓取想要的数据--------------------------------------
    #这里有时是由空页面导致报错，当页面里面为空时活请求的页面报错事，head里面的title值可能会是空，或者"百合网"，所以补货一下异常进行跳过处理
    try:
        title = soup.find("head").find("title").text  # 获取titlBVCXZBVBZXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXBCV
    except Exception:
        title=""

    print(title)
    if title!="百合网" :
        if title:#为空判断
            div_BAIHE =soup.find("body").find(id='BAIHE')    #存储了所有信息的标签
            div_up=div_BAIHE.find("div",attrs="profile").find("div",attrs="userData fixfloat")  #获取包含上部分内容的标签-含数据&图片
            #获取女性用户的womanData属性，若为空，则为男性---男性和女性的用户的网页结构
            div_up_woman = div_up.find("div",attrs="womanData")
            if div_up_woman :  #若不是不为空则是女性
                #抓取womanData中想要的数据
                data=div_up_woman.find_all("div",attrs="inter")[-1].find("p").text
                #获取女生的id
                womanIdText=div_up_woman.find("div",attrs="data").find_all("dl")[0].find_all("dt")[0].text
                id=int(womanIdText.split('：')[1])
                id=id+10000000
                print(id)
            #抓取女生个人的数据
                data_list = data.split('/')  # f分割
                user_age = data_list[0]  # 用户的年龄
                user_height = data_list[1]  # 用户的身高
                user_education = data_list[2]  # 用户的学历
                user_addr = data_list[3]  # 用户的住址
                user_marry = data_list[4]  # 用户的婚况
                user_gender="女"
                # 用户的呢称
                user_name=div_up_woman.find_all("div", attrs="inter")[0].find("div",attrs="name").find_all("span")[1].text

            # 获取女性用户的其他基本信息&求偶意向
                #自我介绍
                self_desc=div_BAIHE.find(id='profileCommon').find("div",attrs="perData").find("div",attrs="intr").text
                # 其他
                all_list = div_BAIHE.find(id='profileCommon').find("div", attrs="perData").find_all("dd")
                user_loc = all_list[0].text  # 户籍
                user_race = all_list[1].text  # 名族
                user_zodiac =all_list[3].text  # 生肖
                user_sign = all_list[4].text  # 星座
                user_shape = all_list[6].text  # 体型
                user_weight = all_list[7].text  # 体重
                user_work = all_list[8].text  # 职业
                user_salary=all_list[9].text #月薪
                user_house=all_list[10].text  #住房
                #print(user_house)
                user_car=all_list[11].text    #是否有车
                user_grad = all_list[13].text  # 毕业院校
                user_major = all_list[14].text  # 专业
                user_religion = all_list[15].text  # 宗教
                user_haschild = all_list[17].text  # 有无小孩
                user_smoke = all_list[19].text  # 是否吸烟
                user_drink = all_list[20].text  # 是否喝酒
                user_firm = all_list[21].text  # 公司

                user_parent = all_list[30].text  # 是否想要和父母住
                user_whenMarry = all_list[31].text  # 何时想结婚
                user_wantkid = all_list[32].text  # 是否想要小孩
                user_meetway = all_list[33].text  # 偏爱的约会方式
                user_cooking = all_list[36].text  # 是否会做饭
                user_housework = all_list[37].text  # 家务
                # 择偶意向
                if all_list[52].text!="不限":
                    o_age = all_list[52].text.rstrip('岁').split('~')  # 对象的年龄
                    o_ageMax = o_age[1]  # 对象的最大年龄
                    o_ageMin = o_age[0]  # 对象的最小年龄
                else:
                    o_ageMax = "不限"  # 对象的最大年龄
                    o_ageMin = "不限"
                    # print(o_ageMax)


                if all_list[53].text!= "不限":
                    o_height = all_list[53].text.rstrip('厘米').split('~')
                    o_heightMax = o_height[1]  # 对象的最大身高
                    o_heightMin = o_height[0]

                else:
                    o_heightMax = "不限"  # 对象的最大身高
                    o_heightMin = "不限"  # 对象的最小身高

                # print(o_heightMax)
                o_education = all_list[54].text  # 对象的学历
                o_salary = all_list[55].text  # 对象的薪资
                o_addr = all_list[56].text  # 对象的所在地
                o_marry = all_list[57].text  # 对象的婚姻状况
                o_hasHouse = all_list[58].text  # 对象是否有房
                o_hasChild = all_list[59].text  # 对象是否有小孩
                o_gender="男"

                #她喜欢什么
                user_like=div_BAIHE.find(id='profileCommon').find("div", attrs="perData").find_all("em")
                all_like=""
                for like in user_like:
                   all_like=all_like+"/"+like.text
                #print(all_like)
                #print(all_list,user_like)

                #抓取woman的图片url
                img_url=""
                try:
                 img_url=div_up.find("div",attrs="womanPic").find("div",attrs="big_pic").find_all("img")[0]['src']
                except Exception:
                    print("没有图片")
                    # 判断图片是否为空，不为空就写入本地
                # 定义队列，将图片的链接和id拼接好，发送过去到队列中，这里其实是在消费者里定义了一个图片消息的生产者，男生部分下同
                    channel.queue_declare(queue='imageChannel')
                if img_url:
                    url = img_url + "-" + str(id)
                    print(type(url))
                    channel.basic_publish(exchange='',
                                          routing_key='imageChannel',
                                          body=url)
                    # 用户的图片
                    img_path = "/" + str(id ) + "/" + str(id ) + ".jpg"

                    print(" [x] Sent %r:%r" % ('imageUrl', '已传送' + url))  # 将数据插入数据库
                else:
                    img_path=""
                    print("无图可传")

                #将数据插入数据库，看看，恶心不恶心

                # userDetail表需要的数据
                value_userDetail = [id,user_weight,user_shape,user_zodiac,user_sign,user_race,user_religion,user_grad,user_major,self_desc]
                value_userDetail_sql='insert into user_detail (id ,weight,shape,zodiac,sign,race,religion,grad_from,major,self_desc) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
                # 择偶意向的数据
                value_spouse = [id,o_gender,o_ageMin,o_ageMax,o_marry,o_addr,o_heightMin,o_heightMax,o_salary,o_hasChild,o_hasHouse]
                value_spouse_sql='insert into spouse (id ,s_gender,s_min_age,s_max_age,s_marital_status,s_home_loc,s_min_height,s_max_height,s_max_salary,s_haschild,s_hashouse) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                #用户信息表
                value_info=[id,user_gender,user_education,user_salary,user_height,user_house,user_haschild,user_age]
                value_info_sql='insert into user_info (id ,gender,education,salary,height,hashouse,haskid,age) values(%s,%s,%s,%s,%s,%s,%s,%s)'
                #用户的喜好
                value_userLike=[id,all_like]
                value_userLike_sql='insert into user_like (id ,hobbies) values(%s,%s)'


                value_picture=[id,img_path]
                value_picture_sql='insert into user_photo (id ,pic1) values(%s,%s)'
                #用户的状态
                value_status=[id,user_work,user_firm,user_smoke,user_drink,user_car,user_cooking,user_housework]
                value_status_sql='insert into user_status (id ,occupation,firm,smoke,drink,hascar,cooking,housework) values(%s,%s,%s,%s,%s,%s,%s,%s)'
                #用户的标签
                value_tag=[id,user_name]
                value_tag_sql='insert into user_tag (id ,name) values(%s,%s)'
                #用户的价值观
                value_userValue=[id,user_whenMarry,user_wantkid,user_parent,user_meetway]
                value_userValue_sql='insert into user_values(id,when_to_marry,want_kid,want_parent,desired_date) values(%s,%s,%s,%s,%s)'

                dataList = [value_userDetail, value_userDetail_sql, value_spouse, value_spouse_sql, value_info, value_info_sql,
                            value_userLike, value_userLike_sql, value_picture, value_picture_sql, value_status,
                            value_status_sql, value_tag, value_tag_sql, value_userValue, value_userValue_sql]
                # # -----------------------------------------------------data-----------------------------------------------------------------------------
               #在这里写入数据库
                with connection.cursor() as cursor:
                    # try:
                    cursor.execute(value_userDetail_sql, value_userDetail)
                    connection.commit()
                    cursor.execute(value_spouse_sql, value_spouse)
                    connection.commit()
                    cursor.execute(value_info_sql, value_info)
                    connection.commit()
                    cursor.execute(value_userLike_sql, value_userLike)
                    connection.commit()
                    cursor.execute(value_picture_sql, value_picture)
                    connection.commit()
                    cursor.execute(value_status_sql, value_status)
                    connection.commit()
                    cursor.execute(value_tag_sql, value_tag)
                    connection.commit()
                    cursor.execute(value_userValue_sql, value_userValue)
                    connection.commit()
                    print(str(id) + "写入数据库成功")

            else:
                     #用户为男生
                  #抓取男生个人的数据
                data=div_up.find("div",attrs="profileTopRight").find("div",attrs="manData").find("p").text
                manText1=div_up.find("div", attrs="profileTopLeft")
                manText2=manText1.find("div", attrs="manPic").find("dl")
                manText3=manText2 .find_all("dt")[0].text

                #注意这里的冒号为中文的冒号----------------md，比其，排了好久
                id=int(manText3.split('：')[1])
                id=id+10000000
                print(id)

                data_list = data.split('/')  # f分割
                user_age = data_list[0]  # 用户的年龄
                user_height = data_list[1]  # 用户的身高
                user_education = data_list[2]  # 用户的学历
                print(user_education)
                user_addr = data_list[3]  # 用户的住址

                # 用户的昵称
                user_name=div_up.find("div", attrs="profileTopRight").find("div", attrs="name").find_all("span")[1].text


            #获取男性用户的其他基本信息 &求偶意向
                list=div_up.find("div", attrs="profileTopRight").find("div", attrs="manData").find("div",attrs="data").find_all("dd")

                user_house=list[0].text  #住房
                user_car=list[1].text    #是否有车
                user_zodiac=list[2].text #生肖
                #user_loc=list[3].text   #家乡
                user_salary=list[4].text #月薪
                user_work=list[5].text   #职业
                user_sign=list[6].text   #星座
                user_marry=list[7].text  #用户的婚况
                user_gender="男"

                #print(user_house,user_car,user_zodiac,user_loc,user_salary,user_work,user_sign,user_marry)
                # 自我介绍
                self_desc = div_BAIHE.find(id='profileCommon').find("div", attrs="perData").find("div", attrs="intr").text
                # 其他
                all_list = div_BAIHE.find(id='profileCommon').find("div", attrs="perData").find_all("dd")
                user_loc =all_list[0].text #户籍
                user_weight=all_list[2].text #体重
                user_shape=all_list[3].text #体型
                user_race=all_list[4].text  #名族
                user_religion=all_list[6].text #宗教
                user_haschild=all_list[8].text #有无小孩
                user_smoke=all_list[9].text #是否吸烟
                user_drink=all_list[10].text #是否喝酒
                user_grad=all_list[11].text  #毕业院校
                user_major=all_list[12].text #专业
                user_firm=all_list[13].text  #公司性质
                user_parent=all_list[23].text #是否想要和父母住
                user_whenMarry=all_list[24].text#何时想结婚
                user_wantkid=all_list[25].text #是否想要小孩
                user_meetway=all_list[26].text #偏爱的约会方式
                user_cooking=all_list[29].text #是否会做饭
                user_housework=all_list[30].text #家务
            #择偶意向
                if all_list[31].text!="不限":
                    o_age=all_list[31].text.rstrip('岁').split('~') #对象的年龄
                    o_ageMax=o_age[1]   #对象的最大年龄
                    o_ageMin=o_age[0]   #对象的最小年龄
                else:
                    o_ageMax = "不限"  # 对象的最大年龄
                    o_ageMin = "不限"
                #print(o_ageMax)
                if all_list[32].text != "不限":
                    o_height=all_list[32].text.rstrip('厘米').split('~')
                    o_heightMax=o_height[1] #对象的最大身高
                    o_heightMin=o_height[0] #对象的最小身高
                else:
                    o_heightMax = "不限"  # 对象的最大身高
                    o_heightMin = "不限"  # 对象的最小身高
                #print(o_heightMax)
                o_education=all_list[33].text #对象的学历
                o_salary=all_list[34].text #对象的薪资
                o_addr=all_list[35].text#对象的所在地
                o_marry=all_list[36].text #对象的婚姻状况
                o_hasHouse=all_list[37].text #对象是否有房
                o_hasChild=all_list[38].text #对象是否有小孩
                o_gender="女"

                # 他喜欢什么
                user_like = div_BAIHE.find(id='profileCommon').find("div", attrs="perData").find_all("em")
                all_like=""
                for like in user_like:
                    all_like=all_like+"/"+like.text
                #print(all_like)

                #print(all_list, user_like)

                # 抓取男生的图片 &入库
                #抓取man图片的url
                img_url=""
                try:
                    img_url=div_up.find("div", attrs="profileTopLeft").find("div", attrs="manPic").find("div",attrs="big_pic").find_all("img")[0]['src']
                except Exception:
                    print("没有图片")
    #-----------------------------------------------------图片-----------------------------------------------------------------------------
                # 判断图片是否为空，不为空就写入本地
                #定义一个消息队例，这里其实是在消费者里定义了一个图片消息的生产者，男生部分下同
                    channel.queue_declare(queue='imageChannel')
                if img_url:
                    url=img_url + "-" + str(id)
                    channel.basic_publish(exchange='',
                                          routing_key='imageChannel',
                                          body=url)

                    print(" [x] Sent %r:%r" % ('imageUrl', '已传送'+url))  # 将数据插入数据库
                    img_path = "/" + str(id ) + "/" + str(id ) + ".jpg"
                else :
                    img_path=""
                    print("无图可传")

                # userDetail表需要的数据
                value_userDetail = [id, user_weight, user_shape, user_zodiac, user_sign, user_race, user_religion,
                                    user_grad, user_major, self_desc]

                value_userDetail_sql = 'insert into user_detail (id ,weight,shape,zodiac,sign,race,religion,grad_from,major,self_desc) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '


                # 择偶意向的数据
                value_spouse = [id, o_gender, o_ageMin, o_ageMax, o_marry, o_addr, o_heightMin, o_heightMax, o_salary,
                                o_hasChild, o_hasHouse]

                value_spouse_sql = 'insert into spouse (id ,s_gender,s_min_age,s_max_age,s_marital_status,s_home_loc,s_min_height,s_max_height,s_max_salary,s_haschild,s_hashouse) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

                # 用户信息表
                value_info = [id, user_gender, user_education, user_salary, user_height, user_house, user_haschild,
                              user_age]

                value_info_sql = 'insert into user_info (id ,gender,education,salary,height,hashouse,haskid,age) values(%s,%s,%s,%s,%s,%s,%s,%s)'


                # 用户的喜好
                value_userLike = [id, all_like]
                value_userLike_sql = 'insert into user_like (id ,hobbies) values(%s,%s)'
                # 用户的图片



                value_picture = [id, img_path]
                value_picture_sql = 'insert into user_photo (id ,pic1) values(%s,%s)'
                # 用户的状态
                value_status = [id, user_work, user_firm, user_smoke, user_drink, user_car, user_cooking,
                                user_housework]
                value_status_sql = 'insert into user_status (id ,occupation,firm,smoke,drink,hascar,cooking,housework) values(%s,%s,%s,%s,%s,%s,%s,%s)'
                # 用户的标签
                value_tag = [id, user_name]
                value_tag_sql = 'insert into user_tag (id ,name) values(%s,%s)'
                # 用户的价值观
                value_userValue = [id, user_whenMarry, user_wantkid, user_parent, user_meetway]
                value_userValue_sql = 'insert into user_values(id,when_to_marry,want_kid,want_parent,desired_date) values(%s,%s,%s,%s,%s)'
                with connection.cursor() as cursor:
                     # try:
                     cursor.execute(value_userDetail_sql, value_userDetail)
                     connection.commit()
                     cursor.execute(value_spouse_sql, value_spouse)
                     connection.commit()
                     cursor.execute(value_info_sql, value_info)
                     connection.commit()
                     cursor.execute(value_userLike_sql, value_userLike)
                     connection.commit()
                     cursor.execute(value_picture_sql, value_picture)
                     connection.commit()
                     cursor.execute(value_status_sql, value_status)
                     connection.commit()
                     cursor.execute(value_tag_sql, value_tag)
                     connection.commit()
                     cursor.execute(value_userValue_sql, value_userValue)
                     connection.commit()
                     print(str(id) + "写入数据库成功")
# 告诉rabbitmq使用callback来接收信息，这里其实是一个消费者，消费来自requestMQ中的消息
channel.basic_consume(callback,
                      queue='hello_baihe',
                      no_ack=True)
 # no_ack=True表示在回调函数中不需要发送确认标识

print(' [*] Waiting for messages. To exit press CTRL+C')

# 开始接收信息，并进入阻塞状态，队列里有信息才会调用callback进行处理。按ctrl+c退出。
channel.start_consuming()


