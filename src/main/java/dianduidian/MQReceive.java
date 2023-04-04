package dianduidian;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.stream.Stream;

public class MQReceive {

    // 连接账号
    private String userName = "";
    // 连接密码
    private String password = "";
    // 连接地址
//    private String brokerURL = "tcp://192.168.31.220:61616";
//    private String brokerURL ="failover:(tcp://192.168.31.219:61616,tcp://192.168.31.220:61616,tcp://191.168.31.218:61616)";
//    private String brokerURL ="failover:(tcp://192.168.31.218:62616,tcp://192.168.31.220:62616,tcp://191.168.31.219:62616)";
    //访问到主节点隔了一个ip 比较慢  ip在前最快
//    private String brokerURL ="failover:(tcp://192.168.31.219:62616,tcp://192.168.31.218:62616,tcp://191.168.31.220:62616)";

//    private String brokerURL ="failover:(tcp://192.168.31.218:62616,tcp://192.168.31.219:62616,tcp://191.168.31.220:62616)";
    private String brokerURL ="failover:(tcp://192.168.31.218:62616,tcp://192.168.31.219:62616,tcp://191.168.31.220:62616,tcp://192.168.31.218:61616,tcp://192.168.31.219:61616,tcp://191.168.31.220:61616)";
    private ActiveMQConnectionFactory factory;//使用Object时使用  需要
//    private ConnectionFactory factory;
    // 连接对象
    private Connection connection;
    // 一个操作会话
    private Session session;
    // 目的地，其实就是连接到哪个队列，如果是点对点，那么它的实现是Queue，如果是订阅模式，那它的实现是Topic
    private Destination destination;
    // 消费者，就是接收数据的对象
    private MessageConsumer consumer;

    public static void main(String[] args) throws JMSException {
        MQReceive  mqReceive=new MQReceive();
        mqReceive.start();

        mqReceive.getTextMessage();//text 文本类型
//        mqReceive.getObjectMessage();// Object
//        mqReceive.getMapMessage();
//          mqReceive.getByteMessage();
//          mqReceive.getStreamMessage();

    }

    public void start() throws JMSException {

        factory = new ActiveMQConnectionFactory(userName,password,brokerURL);
//        factory.setTrustAllPackages(true);//使用Object时使用  需要信任实体类

        connection=factory.createConnection();
        connection.start();


        // Session.AUTO_ACKNOWLEDGE 为自动确认，客户端发送和接收消息不需要做额外的工作。哪怕是接收端发生异常，也会被当作正常发送成功。

        // Session.CLIENT_ACKNOWLEDGE  在接收时使用 为客户端确认。客户端接收到消息后，必须调用javax.jms.Message的acknowledge方法。jms服务器才会当作发送成功，并删除消息。

        // DUPS_OK_ACKNOWLEDGE 允许副本的确认模式。一旦接收方应用程序的方法调用从处理消息处返回，会话对象就会确认消息的接收；而且允许重复确认。

        session=connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
//        session=connection.createSession(false,Session.AUTO_ACKNOWLEDGE);//自动

        //点对点
        Queue queue=session.createQueue("chuan2");

        //订阅
//        destination=session.createTopic("aa");

        consumer=session.createConsumer(queue);

    }



    public  void getTextMessage() throws JMSException {
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    String text=((TextMessage)message).getText();

                    System.out.println("接受消息:" + text);
                    message.acknowledge();//消费 确认
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void getObjectMessage() throws JMSException {
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    User user=(User)((ObjectMessage)message).getObject();
                    System.out.println("接收消息" + user.toString());
//                        message.acknowledge();//消费 确认
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public  void getMapMessage() throws JMSException {

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {

                MapMessage map= (MapMessage) message;
                try {

                String name =map.getString("name");
                int id=map.getInt("id");

                System.out.println("姓名:" + name + "  id:" + id);

                    // 关闭接收端，也不会终止程序哦
                    // consumer.close();

            } catch (JMSException e) {
                e.printStackTrace();
            }
            }
        });
    }




    public  void getByteMessage() throws JMSException {

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {

                FileOutputStream outputStream=null;
                try {
                    BytesMessage bytesMessage= (BytesMessage) message;
                    outputStream=new FileOutputStream("C:\\Users\\user\\Desktop\\新建文本文档.txt");

                    byte[] bytes=new byte[1024];
                    int len=0;
                    while ((len =bytesMessage.readBytes(bytes)) != -1 ){
                        outputStream.write(bytes,0,len);
                    }

                    outputStream.close();
                    System.out.println("写入成功 ");

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }





    //接收流
    public  void getStreamMessage() throws JMSException {

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                StreamMessage streamMessage=(StreamMessage) message;

                try {
                   String name=streamMessage.readString();
                   int id= streamMessage.readInt();
                    System.out.println("name "+name +" id " +id);
                    System.out.println("stream 消息接收成功");
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
    }



}
