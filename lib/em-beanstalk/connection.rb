module EM
  class Beanstalk
    class Connection < EM::Connection
      attr_accessor :client
  
      def connection_completed
        client.connected
      end

      def receive_data(data)
        client.received(data)
      end
  
      def send(command, *args)
        real_send(command, args, nil)
      end
  
      def send_with_data(command, data, *args)
        real_send(command, args, data)
      end
  
      def unbind
        client.disconnected
      end
      
      protected
      def real_send(command, args, data = nil)
        send_data(command.to_s)
        args.each{ |a| send_data(' '); send_data(a) }
        send_data("\r\n")
        if data
          send_data(data)
          send_data("\r\n")
        end
      end
    end
  end
end