package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.*;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import java.util.Scanner;
import java.io.IOException;

public class Chat {

  private static String destino = "";
  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("3.95.195.126");
    factory.setUsername("admin");
    factory.setPassword("password");
    factory.setVirtualHost("/");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    
    Scanner scan = new Scanner(System.in);
    
    System.out.print("User: ");
    String usuario = scan.nextLine();
    
    channel.queueDeclare(usuario, false, false, false, null);
    
    Consumer consumer = new DefaultConsumer(channel) {
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String msg = new String(body, "UTF-8");
        System.out.println("\n" + msg);
        if(destino.isEmpty()){
          System.out.print("<< ");
        } else {
          System.out.print("@"+destino+"<< ");
        }
      }
    };
    channel.basicConsume(usuario, true, consumer);
    
    while(true){
      if(destino.isEmpty()){
        System.out.print("<< ");
      } else {
        System.out.print("@"+destino+"<< ");
      }
      
      String msg = scan.nextLine();
      
      if(msg.startsWith("@")){
        destino = msg.substring(1);
      } else if(!destino.isEmpty()){
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("America/Sao_Paulo"));
        DateTimeFormatter formato = DateTimeFormatter.ofPattern("dd/MM/yyyy 'às' HH:mm");
        String horario = now.format(formato);
        String mensagemFormatada = "(" + horario + ") " + usuario + " diz: " + msg;
        channel.basicPublish("", destino, null, mensagemFormatada.getBytes("UTF-8"));
      } else {
        break;
      }
    }
    
    channel.close();
    connection.close();
    scan.close();
  }
}