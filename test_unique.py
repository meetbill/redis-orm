#!/usr/bin/python
#coding=utf8
"""
# Author: meetbill
# Created Time : 2018-05-22 15:35:44

# File Name: test.py
# Description:

"""
import redisorm
import time

redisorm.setup_redis("default","127.0.0.1",6379)
print redisorm.SYSTEMS
#
#
class User(redisorm.Model):
    unique_field = "name"
    exclude_attrs = ["desc"]
    pass

#User.set_unique("name")


print "########################## create"
print User.objects.create(name='wangbin', age=26,desc="wangbin_desc")
print User.objects.create(name='meetbill', age=26,desc="meetbill_desc")
print User.objects.create(name='meetbill_expire1', age=26,expire=3,desc="meetbill_expire_desc1")
print User.objects.create(name='meetbill_expire2', age=26,expire=3,desc="meetbill_expire_desc2")
print User.objects.create(name='meetbill', age=25,desc="meetbill")


print "########################## find"
print User.objects.find(age=26)
print User.objects.find(age=26).list()
print User.objects.find(age=26).list()[0].tags


print "########################## [get data]"
for info in User.objects.find(age=26).list():
    print "attrs:[%s] expire:[%s]"%(info.attrs,User.ttl(info))
print "-------------------------- [get data] test expire"
time.sleep(4)
for info in User.objects.find(age=26).list():
    print info.attrs


print "########################## all"
for info in User.objects.all().list():
    print info.attrs


print "########################## delete_instance"
for info  in  User.objects.all().list():
    print "[id]%s [tags]%s \n"%(info.id,info.tags)
    User.objects.delete_instance(info)
print "########################## delete_instance expire"
User.objects.expire()

