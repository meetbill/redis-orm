#!/usr/bin/python
# coding=utf8
"""
# Author: meetbill
# Created Time : 2018-05-22 15:35:44

# File Name: test.py
# Description:

"""
import redisorm
import time

redisorm.setup_redis("default", "127.0.0.1", 6379)
print redisorm.SYSTEMS


class User(redisorm.Model):
    pass


print "########################## create"
print User.objects.create(name='wangbin', age=26)
print User.objects.create(name='meetbill', age=26)
print User.objects.create(name='meetbill_expire', age=26, expire=3)
print User.objects.create(name='meetbill', age=25)


print "########################## find"
print User.objects.find(age=26)
print User.objects.find(age=26).list()
print User.objects.find(age=26).list()[0].tags


print "########################## [get data]"
for info in User.objects.find(age=26).list():
    print "[attrs]:%s [expire]:%s" % (info.attrs, User.ttl(info))

print "-------------------------- [get data] test expire"
time.sleep(4)
for info in User.objects.find(age=26).list():
    print info.attrs


print "########################## all"
for info in User.objects.all().list():
    print info.attrs


print "########################## delete_instance"
for info in User.objects.all().list():
    print "[id]:%s [tags]:%s \n" % (info.id, info.tags)
    print User.objects.delete_instance(info)
