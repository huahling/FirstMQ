package dianduidian;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class MQproducer {

    private String userName="";
    private String password="";

//    private String brokerURL ="tcp://192.168.31.220:61616";//单个时使用
//private String brokerURL ="failover:(tcp://192.168.31.220:61616,tcp://192.168.31.219:61616,tcp://191.168.31.218:61616)";

//访问到主节点隔了一个ip 比较慢  ip在前最快
    private String brokerURL ="failover:(tcp://192.168.31.218:61616,tcp://192.168.31.219:61616,tcp://191.168.31.220:61616)";
//    private String brokerURL ="failover:(tcp://192.168.31.220:61616,tcp://192.168.31.219:61616,tcp://191.168.31.218:61616)";
//    private String brokerURL ="failover:(tcp://192.168.31.218:61616,tcp://192.168.31.219:61616,tcp://191.168.31.220:61616,tcp://192.168.31.218:62616,tcp://192.168.31.219:62616,tcp://191.168.31.220:62616)";

    private ActiveMQConnectionFactory factory;
//    private ConnectionFactory factory;
    private Connection connection;
    private Session session;
    private Destination destination;
    private MessageProducer producer;

    public static void main(String[] args) throws JMSException {

        MQproducer mQproducer=new MQproducer();
        mQproducer.start();

        mQproducer.sentMessage();
//        mQproducer.sendObjectMessage();
//        mQproducer.setMapMessage();
//        mQproducer.setByteMessage();
//        mQproducer.setSteamMessage();

    }

    public  void start() throws JMSException {

        try {
             factory = new ActiveMQConnectionFactory(userName,password,brokerURL);
//            factory.setTrustAllPackages(true);

            connection = factory.createConnection();
            //是否支持事务true false    true 则忽略第二个参数  会被设计为SESSION_TRANSACTED
            //false  Session.AUTO_ACKNOWLEDGE  自动确认， Session.CLIENT_ACKNOWLEDGE， DUPS_OK_ACKNOWLEDGE其中一个。

            // Session.AUTO_ACKNOWLEDGE 为自动确认，客户端发送和接收消息不需要做额外的工作。哪怕是接收端发生异常，也会被当作正常发送成功。


            // Session.CLIENT_ACKNOWLEDGE  在接收时使用 为客户端确认。客户端接收到消息后，必须调用javax.jms.Message的acknowledge方法。jms服务器才会当作发送成功，并删除消息。
            //需要确认  消费者时使用

            // DUPS_OK_ACKNOWLEDGE 允许副本的确认模式。一旦接收方应用程序的方法调用从处理消息处返回，会话对象就会确认消息的接收；而且允许重复确认。

            //开启事务，会回滚 这里是发送消息
//            session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);//消费者时使用
//            session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);//需要提交
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);//自动确认 不用客户端确认也可以

            //队列  有就使用以前的，没有就创建  点对点
            Queue queue=session.createQueue("chuan2");

            //订阅
//            destination = session.createTopic("aa");

            //创建生产者
            producer = session.createProducer(queue);

        } catch (JMSException e) {
            e.printStackTrace();
        }

    }

        public void sentMessage(){
            try {
                //创建消息
                TextMessage textMessage = session.createTextMessage();
                for (int i = 1; i <= 1000; i++) {
                    textMessage.setText("消息" + i);
                    System.out.println("消息"+i);

                    producer.send(textMessage);
                }
                //已经消费过消息，但没有被删除  需要确认  才能被删除   没被消费，还能被消费
//                session.commit();//客户端 提交 确认
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        public void sendObjectMessage() throws JMSException {
    //创造消息
        for (int i =1;i<=10 ; i++){
            User user=new User();
            user.setId(i);
            user.setName("name"+i);
            user.setAddr("addr"+i);
            if(i % 2==0){
                user.setSex("男");

            }else{
                user.setSex("女");
            }

            ObjectMessage objectMessage=session.createObjectMessage();
            objectMessage.setObject(user);

            System.out.println("序列化对象发送成功");
            producer.send(objectMessage);
        }

        }


    public void setMapMessage() throws JMSException {

        for (int i = 1; i <= 10; i++) {
            String name = "name";
            MapMessage message = session.createMapMessage();
            message.setString("name", name + i);
            message.setInt("id", i);

            System.out.println("发送的消息 name "+name+i+" id "+i);

            producer.send(message);
        }

    }


    public void setByteMessage() throws JMSException {

        byte [] bytes="shishinanliao".getBytes();
        BytesMessage bytesMessage=session.createBytesMessage();
        bytesMessage.writeBytes(bytes);
        producer.send(bytesMessage);
        System.out.println("消息发送成功 ");

    }


    public void setSteamMessage() throws JMSException {
        StreamMessage streamMessage=session.createStreamMessage();
        String name ="name";
        for (int i = 1; i <= 10; i++) {
            streamMessage.writeString(name+i);
            streamMessage.writeInt(i);

            producer.send(streamMessage);
            System.out.println("Stream 消息发送成功");

        }

    }





}
