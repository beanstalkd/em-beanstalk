module EventMachine
  class Beanstalk
    class Defer < EM::DefaultDeferrable
      def initialize(default_error_callback, &block)
        @error = default_error_callback
        callback(&block) if block
        errback {|message| @error.call(message)}
      end
      
      def error(&block)
        @error = block
        self
      end
      
    end
  end
end