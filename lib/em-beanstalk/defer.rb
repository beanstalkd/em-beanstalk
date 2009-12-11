module EventMachine
  class Beanstalk
    class Defer < EM::DefaultDeferrable
      def initialize(default_error_callback, &block)
        @error = default_error_callback
        callback(&block) if block
        errback{|message| @error.call(message)}
      end
      
      def on_error(&block)
        @error = block
        self
      end
      
      def on_success(&block)
        callback(&block)
        self
      end
      
    end
  end
end