#
# qpid_punisher.rb -- Qpid and AMQP server test
#
# Author: Colin Surprenant
#         http://github.com/colinsurprenant/
#         http://eventuallyconsistent.com/blog/
#
# Revision: 1.1
# Date: May 5, 2009
#
# Requires the Qpid Gem available at:
#   http://github.com/colinsurprenant/qpid
#
# The Qpid Gem can be installed directly using:
#   sudo gem install colinsurprenant-qpid -s http://gems.github.com
#
# This is an example of Qpid and AMQP server usage. This was written to stress test the qpid gem and RabbitMQ.
# To use it simply open two terminal windows and in the first type:
#   ruby qpid_punisher -c 
# and in the other type:
#   ruby qpid_punisher -p
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
require "qpid"
require 'getoptlong'

module BunnyPunisher
  
  ITEMS_PER_ITERATION = 1000
  MAX_ITERATIONS = 10
  EMPTY_HEADER = {}
  
  def self.connect
    spec  = "#{Gem.loaded_specs['colinsurprenant-qpid'].full_gem_path}/specs/official-amqp0-8.xml"
    host  = '127.0.0.1'
    port  = 5672
    vhost = '/'
    user  = 'guest'
    pass  = 'guest'
            
    puts("connecting to #{host}:#{port} on vhost #{vhost} with user=#{user}, pass=#{pass}")

    client = Qpid::Client.new(host, port, Qpid::Spec::Loader.build(spec), vhost)
    client.start({ "LOGIN" => user, "PASSWORD" => pass })
    channel = client.channel(1)
    channel.channel_open
    
    return channel, client
  end
  
  def self.close(channel, client)
    puts("closing channel and client")

    channel.channel_close
    client.close
  end
  
  def self.qpid_publish(channel, key, data)
    c = Qpid::Content.new(EMPTY_HEADER, Marshal.dump(data))
    channel.basic_publish(:routing_key => key.to_s, :content => c, :exchange => 'amq.direct')    
  end
  
  def self.bench
    time_start = Time.now    
    yield
    puts("sent #{ITEMS_PER_ITERATION} in #{(Time.now - time_start).to_s} sec")
  end
            
  def self.publish(channel, key, value)
    puts("publishing on queue=#{key}, data=#{value.to_s}")

    (1..MAX_ITERATIONS).each do
      self.bench do
        self.qpid_publish(channel, key, "begin")
        (1..ITEMS_PER_ITERATION).each { self.qpid_publish(channel, key, value) }
        self.qpid_publish(channel, key, "end")
      end
      sleep(2) # allow qpid queues to empty
    end
    
    self.qpid_publish(channel, key, "stop")    
  end
  
  def self.consume(channel, client, key)
    puts("consuming on queue=#{key}")
    
    channel.queue_declare(:queue => key)
    channel.queue_bind(:queue_name => key, :exchange => 'amq.direct')
    bc = channel.basic_consume(:queue => key)
    queue = client.queue(bc.consumer_tag)
    
    i = 0
    time_start = Time.now
    loop do
      message = queue.pop(non_block = false)
      channel.basic_ack(message.delivery_tag)
      value = Marshal.load(message.content.body) 
#      puts("received value=#{value.inspect}")
      
      case value
      when "begin"
        i = 0
        time_start = Time.now
      when "end"
        puts("received #{i - 1} in #{(Time.now - time_start).to_s} sec")
      end
    
      i += 1
      break if value == "stop"
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

    channel, client = connect
    key = "test_queue"
    data = "test data"
    
    case mode
    when :consumer
      consume(channel, client, key)
    when :publisher      
      publish(channel, key, data)
    end
    
    close(channel, client)
  end
end

puts("starting Qpid Punisher")
BunnyPunisher::main
puts("exiting Qpid Punisher")