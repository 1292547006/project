/**
* @MessageBody.h
* @brief  消息结构实体类
*
*         详细: 将RabbitMQ 的访问API 用C++ 封装
*  @date      2013-9-10
*  @author    陈梵
*  @version   1.3
*  @note	  内部使用
* 
*  Copyright (c) 2011 江苏省邮电规划设计院  (版权声明)
*  All rights reserved.
**/
#ifndef PRODUCER_H_CREATEDBYCHENFAN_MESSAGE_BODY_20130910_JSPTPD
#define PRODUCER_H_CREATEDBYCHENFAN_MESSAGE_BODY_20130910_JSPTPD

#include "SysDefine.h"

#include "stdint.h"
#include <vector>
#include <string>
using namespace std;

/** 
*   @brief 消息队列的消息实体
* 
*   class Message in "MessageBody.h"
**/
class Publish CMessage
{
public:
	CMessage(string data,int32_t durable=2):
	  m_data(data),m_durable(durable)
	{
		this->m_type = "text/plain";
	}
    CMessage(string data,string routekey,int32_t durable=2):
	  m_data(data),m_routekey(routekey),m_durable(durable)
	{
		this->m_type = "text/plain";
	}
	//拷贝构造函数
	CMessage(const CMessage &other)
	{
		this->m_data    = other.m_data;
		this->m_durable = other.m_durable;
	    this->m_type    = other.m_type;
	    this->m_routekey =other.m_routekey;
	}
    //重载 赋值= 运算符
	CMessage operator=(const CMessage &other)
	{
		if(this==&other) return *this; 
		this->m_data    = other.m_data;
		this->m_durable = other.m_durable;
	    this->m_type    = other.m_type;
		this->m_routekey =other.m_routekey;
		return *this;
	}

    string  m_data;             //消息内容，目前只支持文本，所以采用string
    int32_t m_durable;          //消息是否持久化; 1 不持久化; 2 持久化
    string  m_type;             //消息类型  目前只支持文本类型 "text/plain"
	string  m_routekey;         //消息路由 默认所有消息的路由都以 “msg.”开头
    //size_t  length;           //消息长度
    //void    *data;            //消息内容
};


/** 
*   @brief 消息队列的队列实体
* 
*   class Message in "RabbitMQ.h"
**/
class Publish CQueue
{
public:
	CQueue(string name,int32_t durable=1):
	  m_name(name),m_durable(durable)
	  {
		  m_auto_delete = 0;
		  m_exclusive = 0;
		   m_passive = 0;
	  }
//拷贝构造函数
	CQueue(const CQueue &other)
	{
		this->m_name		     = other.m_name;
		this->m_durable			 = other.m_durable;
		this->m_auto_delete      = other.m_auto_delete;
		this->m_exclusive        = other.m_exclusive  ;
		this->m_passive          = other.m_passive    ;
	}
  //重载 赋值= 运算符
	CQueue operator=(const CQueue &other)
	{
		if(this==&other) return *this; 
		this->m_name		     = other.m_name;
		this->m_durable			 = other.m_durable;
		this->m_auto_delete      = other.m_auto_delete;
		this->m_exclusive        = other.m_exclusive  ;
		this->m_passive          = other.m_passive    ;
		return *this;
	}
	string   m_name;         //消息队列名称
	int32_t  m_durable;      //消息队列是否持久化
	int32_t  m_exclusive;    //当前连接不在时，队列是否自动删除
	int32_t  m_auto_delete;  //没有consumer时，队列是否自动删除
	int32_t  m_passive; 
};

/** 
*   @brief 消息队列的队列实体
* 
*   class Message in "RabbitMQ.h"
**/
class Publish CExchange
{
public:
	CExchange(string name,int32_t durable=1,string type="direct"):
        m_name(name),m_durable(durable),m_type(type)
	{
		 m_passive = 0;
	}
    //拷贝构造函数
	CExchange(const CExchange &other)
	{
		this->m_name		     = other.m_name;
		this->m_durable			 = other.m_durable;
	    this->m_type	         = other.m_type;
		this->m_passive          = other.m_passive;
	}
   //重载 赋值= 运算符
	CExchange operator=(const CExchange &other)
	{
		if(this==&other) return *this; 
		this->m_name		     = other.m_name;
		this->m_durable			 = other.m_durable;
	    this->m_type	         = other.m_type;
		this->m_passive          = other.m_passive;
		return *this;
	}
	string   m_name;         //交换机名称
	string   m_type;         //交换机类型  "fanout"  "direct" "topic"三选一
	int32_t  m_durable;      //交换机是否持久化
	int32_t  m_passive;          

};


#endif