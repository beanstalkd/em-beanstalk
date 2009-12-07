module EventMachine
  class Beanstalk
    class Defer < EM::DefaultDeferrable
      def initialize(default_error_callback = nil, &block)
        errback(&default_error_callback) if default_error_callback
        callback(&block) if block
      end
      
      def error(&block)
        errback(&block)
        self
      end
    end
  end
end