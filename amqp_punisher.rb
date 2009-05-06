#
# amqp_punisher.rb -- AMQP server test
#
# Author: Colin Surprenant
#         http://github.com/colinsurprenant/
#         http://eventuallyconsistent.com/blog/
#
# Revision: 1.0
# Date: Dec 9, 2008
#
# Requires the tmm1-amqp Gem available at:
#   http://github.com/tmm1/amqp
#
# The tmm1-amqp Gem can be installed directly using:
#   sudo gem install tmm1-amqp -s http://gems.github.com
#
# This is an example of tmm1-amqp and a AMQP server usage. This was written to stress test the tmm1-amqp gem and RabbitMQ.
# To use it simply open two terminal windows and in the first type:
#   ruby amqp_punisher -c 
# and in the other type:
#   ruby amqp_punisher -p
#
# This will start a consumer instance and a publisher instance and the
# publisher will start sending a series of 1000 items that the consumer
# will pick up. Note that the publisher timing info is boggus since it
# actually calculates the time required to fill the qpid internal queue and
# not the time qpid will take to send them to RabbitMQ. On the other hand, 
# the timings on the consumer end are more informative since it indicates
# the time it took to fetch all these items from RabbitMQ.
#

require "rubygems"
require "mq"
require 'getoptlong'

module BunnyPunisher
  
  ITEMS_PER_ITERATION = 1000
  MAX_ITERATIONS = 10
  
  def self.connect(&block)
    options = {
      :host  => '127.0.0.1',
      :port  => 5672,
      :vhost => '/',    
      :user  => 'guest',
      :pass  => 'guest',
      :timeout => nil,
      :logging => false
    }
            
    puts("connecting to #{options[:host]}:#{options[:port]} on vhost #{options[:vhost]} with user=#{options[:user]}, pass=#{options[:pass]}")
    connection = AMQP.connect(options)
    @amq = MQ.new(connection)
    
    yield
  end


  def self.close
    puts("closing")
  end
            
  def self.publish(key, value)
    raise("publish not working yet. note that you can also use qpid_punisher -p")
  end

  def self.subscribe(key)
    @amq.queue(key.to_s).subscribe do |message|
      yield Marshal.load(message)
    end
  end
  
  def self.consume(key)
    puts("consuming on queue=#{key}")
    
    i = 0
    time_start = Time.now
    EM.run do
      self.connect do
        [key].each do |k|
          self.subscribe(k) do |value|
#            puts("received message on key=#{k}, value=#{value.inspect}")
    
            case value
            when "begin"
              i = 0
              time_start = Time.now
            when "end"
              puts("received #{i - 1} in #{(Time.now - time_start).to_s} sec")
            end
          
            i += 1
            EM.stop if value == "stop"        
          end
        end
      end
    end
  end

  def self.main
    opts = GetoptLong.new(
      [ '--consumer', '-c', GetoptLong::NO_ARGUMENT ],
      [ '--publisher', '-p', GetoptLong::NO_ARGUMENT ]
    )

    mode = :consumer # default
    opts.each do |opt, arg|
      case opt
        when '--consumer'
          mode = :consumer
        when '--publisher'
          mode = :publisher
      end
    end

    key = "test_queue"
   
    case mode
    when :consumer
      consume(key)
    when :publisher      
      publish(key, "some data")
    end
    
    close
  end
end

puts("starting AMQP Punisher")
BunnyPunisher::main
puts("exiting AMQP Punisher")