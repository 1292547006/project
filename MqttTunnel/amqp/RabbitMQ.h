/**
* @RabbitMQ.h
* @brief  RabbitMQ API C++ 接口
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
#ifndef PRODUCER_H_CREATEDBYCHENFAN_20130724_JSPTPD
#define PRODUCER_H_CREATEDBYCHENFAN_20130724_JSPTPD

#include "SysDefine.h"
#include <string>
#include <vector>
#include "stdint.h"
#include "MessageBody.h"
using namespace std;

class CRabbitMQ_Adapter;


/** 
*   @brief 消息队列工具类
* 
*   class CRabbitMQ in "RabbitMQ.h"
**/
class Publish CRabbitMQ
{
public:

	/**
	* @brief CRabbitMQ 构造函数
	* @param [int] HostName   消息队列名称
	* @param [int] port       消息队列端口号
	* @return 无
	* @par 示例:
	*  @code
	*  @endcode
    *  @see      
	*  @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/
	CRabbitMQ(string HostName="localhost",uint32_t port=5672,string usr="guest",string psw="guest");
   //析构函数
	~CRabbitMQ();
   
	/**
	* @brief connect  连接消息队列服务器
	* @param [out] ErrorReturn   错误信息
	* @return 等于0值代表成功连接，小于0代表错误，错误信息从ErrorReturn返回
    *  @par 示例:
	*  @code
	*  @endcode
    *  @see      
	*  @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/
	int32_t Connect(string &ErrorReturn=string(""));
    
   /**
	* @brief Disconnect  与消息队列服务器断开连接
	* @param [out] ErrorReturn   错误信息
	* @return 等于0值代表成功断开连接，小于0代表错误，错误信息从ErrorReturn返回
	*
	*  @par 示例:
	*  @code
	*  @endcode
    *  @see      
	*  @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/
   int32_t Disconnect(string &ErrorReturn=string(""));

       /**
	*   @brief       exchange_declare   声明exchange
	*	@param       [in]               exchange       交换机实例
	*   @param        [out] ErrorReturn   错误信息
	*   @return 等于0值代表成功创建exchange，小于0代表错误，错误信息从ErrorReturn返回
	*   @par 示例:
	*   @code
    *   @endcode
    *   @see      
	*   @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/
   int32_t exchange_declare(CExchange &exchange,string &ErrorReturn=string(""));

    /**
	*   @brief       queue_declare                    声明消息队列
	*	@param       [in]               queue         消息队列实例
	*   @param       [out]              ErrorReturn   错误信息
	*   @return 等于0值代表成功创建queue，小于0代表错误，错误信息从ErrorReturn返回
	*   @par 示例:
	*   @code
    *   @endcode
    *   @see      
	*   @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/
   int32_t queue_declare(CQueue &queue,string &ErrorReturn=string(""));

    /**
	*   @brief       queue_bind                       将队列，交换机和绑定规则绑定起来形成一个路由表
	*	@param       [in]               queue         消息队列
	*	@param       [in]               exchange      交换机名称
	*	@param       [in]               bind_key      路由名称  “msg.#”“msg.weather.**”
	*   @param       [out]              ErrorReturn   错误信息
	*   @return 等于0值代表成功绑定，小于0代表错误，错误信息从ErrorReturn返回
	*   @par 示例:
	*   @code
    *   @endcode
    *   @see      
	*   @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/
    int32_t queue_bind(CQueue &queue,CExchange &exchange,const string bind_key,string &ErrorReturn=string(""));

  /**
	*   @brief       queue_bind                       将队列，交换机和绑定规则绑定解除
	*	@param       [in]               queue         消息队列
	*	@param       [in]               exchange      交换机名称
	*	@param       [in]               bind_key      路由名称  “msg.#”“msg.weather.**”
	*   @param       [out]              ErrorReturn   错误信息
	*   @return 等于0值代表成功绑定，小于0代表错误，错误信息从ErrorReturn返回
	*   @par 示例:
	*   @code
    *   @endcode
    *   @see      
	*   @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/
    int32_t queue_unbind(CQueue &queue,CExchange &exchange,const string bind_key,string &ErrorReturn=string(""));

   /**
	* @brief publish  发布消息
	* @param [in] messag         消息实体
	* @param [in] rout_key       路由规则 
    *   1.Direct Exchange – 处理路由键。需要将一个队列绑定到交换机上，要求该消息与一个特定的路由键完全匹配。
    *   2.Fanout Exchange – 不处理路由键。将队列绑定到交换机上。一个发送到交换机的消息都会被转发到与该交换机绑定的所有队列上。
	*   3.Topic Exchange – 将路由键和某模式进行匹配。此时队列需要绑定要一个模式上。符号“#”匹配一个或多个词，符号“*”匹配不多不少一个词。
    *      因此“audit.#”能够匹配到“audit.irs.corporate”，但是“audit.*” 只会匹配到“audit.irs”
	* @param [out] ErrorReturn   错误信息
	* @return 等于0值代表成功发送消息实体个数，小于0代表发送错误，错误信息从ErrorReturn返回
	*
	*  @par 示例:
	*  @code
    *  @endcode
    *  @see      
	*  @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/
   int32_t publish(vector<CMessage> &message,string routkey,string &ErrorReturn=string(""));

   int32_t publish(CMessage &message,string routkey,string &ErrorReturn=string(""));

   int32_t publish(const string &message,string routkey,string &ErrorReturn=string(""));

  /** 
	* @brief consumer  消费消息
	* @param [in]  queue        队列
	* @param [out] message      消息实体
    * @param [int] GetNum       需要取得的消息个数
	* @param [int] timeout      取得的消息是延迟，若为NULL，表示持续取，无延迟，阻塞状态
    * @param [out] ErrorReturn   错误信息
	* @return 大于0代表成功，代表取回的消息个数，小于0代表错误，错误信息从ErrorReturn返回,
	*
	*  @par 示例:
	*  @code
    *  @endcode
    *  @see      
	*  @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/
   int32_t consumer(CQueue &queue,vector<CMessage> &message, uint32_t GetNum=1,struct timeval *timeout=NULL,string &ErrorReturn=string(""));
   int32_t consumer(const string & queue_name,vector<string> &message_array, uint32_t GetNum=1000,struct timeval *timeout=NULL,string &ErrorReturn=string(""));
  

    /**
	*   @brief       queue_delete                     删除消息队列。
	*	@param       [in]               queuename     消息队列名称
	*	@param       [in]               if_unused     消息队列是否在用，1 则论是否在用都删除
	*   @param       [out]              ErrorReturn   错误信息
	*   @return 等于0值代表成功删除queue，小于0代表错误，错误信息从ErrorReturn返回
	*   @par 示例:
	*   @code
    *   @endcode
    *   @see      
	*   @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/
    int32_t queue_delete(const string queuename,int32_t if_unused=0,string &ErrorReturn=string(""));
	
  
	/**
	* @brief getMessageCount      获得队列消息个数 
	* @param [in]  Queue          要获取消息个数的消息队列
    * @param [out] ErrorReturn    错误信息
	* @return 返回-1，代表获取消息个数失败，返回大于等于等于0值代表消息个数，错误信息从ErrorReturn返回
	*
	*  @par 示例:
	*  @code
    *  @endcode
    *  @see      
	*  @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/ 
	int32_t getMessageCount(const CQueue &queue,string &ErrorReturn=string(""));
	int32_t getMessageCount(const string &queuename,string &ErrorReturn=string(""));
   
	/**
	* @brief setUser            设置登录用户名称 
	* @param [in]  UserName     登录用户名称
	*  @par 示例:
	*  @code
    *  @endcode
    *  @see      
	*  @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/ 
    void setUser(const string UserName);

    /**
	* @brief getUser       获得登录用户名称 
	* @return              返回当前登录用户名
	*  @par 示例:
	*  @code
    *  @endcode
    *  @see      
	*  @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/ 
    string getUser() const;

   	/**
	* @brief setPassword        设置登录用户密码 
	* @param [in]  password     登录用户密码
	*  @par 示例:
	*  @code
    *  @endcode
    *  @see      
	*  @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/ 
   void setPassword(const string password);

    /**
	* @brief getPassword   获得登录用户密码 
	* @return              返回当前登录用户名密码
	*  @par 示例:
	*  @code
    *  @endcode
    *  @see      
	*  @deprecated 由于特殊的原因，这个函数可能会在将来的版本中取消。
	*/ 
   string getPassword() const;
	
    void __sleep(uint32_t millsecond);
private:
	CRabbitMQ_Adapter *adapter;
};


#endif

