require_relative "spec_helper"

describe "connection_checkout_event_callback extension" do
  it "should error if using an unsupported connection pool" do
    db = Sequel.mock(:pool_class => :single)
    proc{db.extension(:connection_checkout_event_callback)}.must_raise Sequel::Error
  end
end 

[true, false].each do |sharded|
  describe "connection_checkout_event_callback extension with #{"sharded_" if sharded}timed_queue connection pool" do
    if sharded
      def wrap(x)
        [x, :default]
      end
    else
      def wrap(x)
        x
      end
    end

    it "should issue expected events" do
      opts = {max_connections: 1}
      opts[:servers] = {:a=>{}} if sharded
      db = Sequel.mock(opts)
      db.extension(:connection_checkout_event_callback)

      # Test that default callback does not break anything
      db.synchronize{}
      db.disconnect

      events = []
      if sharded
        db.pool.on_checkout_event = proc{|*event| events << event}
      else
        db.pool.on_checkout_event = proc{|event| events << event}
      end
    
      db.synchronize{}
      events.must_equal [wrap(:not_immediately_available), wrap(:new_connection)]
      events.clear

      q = Queue.new
      q2 = Queue.new
      t = Thread.new do
        db.synchronize do
          q2.push(true)
          _(q.pop(timeout: 1)).must_equal true
        end
        true
      end
      q2.pop(timeout: 1).must_equal true
      events.must_equal [wrap(:immediately_available)]
      events.clear

      t2 = Thread.new do
        db.synchronize do
          q2.push(true)
          _(q.pop(timeout: 1)).must_equal true
        end
        true
      end
      10.times{Thread.pass}
      q.push(true)
      q2.pop(timeout: 1).must_equal true
      q.push(true)
      events[0].must_equal wrap(:not_immediately_available)
      if sharded
        events[1][0].must_be :<, 1
        events[1][1].must_equal :default
      else
        events[1].must_be :<, 1
      end
      t.join(1).value.must_equal true
      t2.join(1).value.must_equal true
    end
  end
end if RUBY_VERSION >= "3.2"
