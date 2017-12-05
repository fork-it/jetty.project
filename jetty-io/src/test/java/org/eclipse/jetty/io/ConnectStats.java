package org.eclipse.jetty.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.eclipse.jetty.toolchain.perf.HistogramSnapshot;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.statistic.CounterStatistic;
import org.eclipse.jetty.util.statistic.SampleStatistic;

public class ConnectStats extends AbstractLifeCycle implements Runnable
{
    private final Selector selector;
    private final Histogram histogram = new Histogram(2);
    private final HistogramSnapshot snapshot = new HistogramSnapshot(histogram);
    private final SampleStatistic samples = new SampleStatistic();
    private final CounterStatistic failures = new CounterStatistic();
    private final CounterStatistic block = new CounterStatistic();
    private final CounterStatistic async = new CounterStatistic();
    private final CounterStatistic direct = new CounterStatistic();
    private final CounterStatistic connections = new CounterStatistic();
    private final boolean blocking;
    private final Queue<Connection> actions = new ArrayDeque<>();
    private boolean selecting;
    private boolean dump;
    
    public ConnectStats(boolean blocking) throws Exception
    {
        this.selector = Selector.open();
        this.blocking = blocking;
    }
    
    @Override
    public void doStart() throws Exception
    {
        new Thread(this).start();
    }

    
    @Override
    public void doStop() throws Exception
    {
       System.err.println(this); 
    }
    
    @Override
    public String toString()
    {
        return String.format("%,8d /%,9d connections, %,10d mean, %,12d max, %d/%d/%d block/async/direct, %d failures",
                samples.getCount(),connections.getCurrent(),(long)samples.getMean(),samples.getMax(),
                block.getCurrent(),async.getCurrent(),direct.getCurrent(), failures.getCurrent());
    }
    
    public void blockingConnect(InetSocketAddress address) throws Exception
    {
        AsynchronousSocketChannel client = AsynchronousSocketChannel.open();
        
        client.connect(address).get(5, TimeUnit.SECONDS);
    }
    
    public void connect(InetSocketAddress address)
    {
        try
        {
            Connection connection = new Connection();
            connection.channel.configureBlocking(blocking);

            if (blocking)
            {
                connection.channel.socket().connect(address,30000);
                connection.channel.configureBlocking(false);
                block.increment();
                connection.connected();
            }
            else
            {
                connection.channel.configureBlocking(false);
                if (connection.channel.connect(address))
                {
                    direct.increment();
                    connection.connected();
                }
                else
                {
                    async.increment();
                }
            }

            synchronized (this)
            {
                actions.add(connection);
                if (selecting)
                    selector.wakeup();
                selecting = false;
            }
        }
        catch(Exception e)
        {
            System.err.println("Connection failure: "+address);
            e.printStackTrace();
        }

    }
    
    @Override
    public void run()
    {
        while(isRunning())
        {
            synchronized (this)
            {
                if (dump)
                {
                    dump = false;
                    System.err.println(snapshot.toString());
                }
                
                for (Connection connection : actions)
                {
                    if (connection.isConnected())
                    {
                        try
                        {
                            connection.channel.register(selector,SelectionKey.OP_READ,connection);
                            histogram.recordValue(connection.connected-connection.initiated);
                        }
                        catch (ClosedChannelException e)
                        {
                            e.printStackTrace();
                        }
                    }
                    else
                    {
                        try
                        {
                            connection.channel.register(selector,SelectionKey.OP_CONNECT,connection);
                        }
                        catch (ClosedChannelException e)
                        {
                            e.printStackTrace();
                        }
                    }
                }
                actions.clear();
                selecting = true;
            }
            

            try
            {
                selector.select();
                
                for (SelectionKey key: selector.selectedKeys())
                {
                    Connection connection = (Connection)key.attachment();
                    
                    if (key.isConnectable())
                    {
                        if (connection.channel.finishConnect())
                        {
                            connection.connected();
                            key.interestOps(SelectionKey.OP_READ);
                            histogram.recordValue(connection.connected-connection.initiated);
                        }
                        else
                        {
                            connection.close();
                        }
                    }
                    else if (key.isReadable())
                    {
                        connection.close();
                        key.cancel();
                    }
                }
                
                selector.selectedKeys().clear();
                
                
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }
    
    private static void usage()
    {
        System.err.println("Usage: java -jar connect.jar <true|false> <threads> <port> <rate C/T/s> <addr...>");
        System.exit(1);
    }

    public static void main(String... args) 
    {
        try
        {
            if (args.length<5)
                usage();

            final boolean blocking = Boolean.valueOf(args[0]);
            final int threads = Integer.valueOf(args[1]);
            final int port = Integer.valueOf(args[2]);
            final int rate = Integer.valueOf(args[3]);
            final InetSocketAddress[] addresses = new InetSocketAddress[args.length-4];
            for (int i=0;i<addresses.length;i++)
                addresses[i] = new InetSocketAddress(args[i+4],port);            

            final ConnectStats connect = new ConnectStats(blocking);
            connect.start();

            for (int i=0; i<threads; i++)
            {
                new Thread(()->
                {
                    long started = System.nanoTime();
                    long created = 0;
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    while (connect.isRunning())
                    {
                        try
                        {
                            Thread.sleep(random.nextInt(1000));
                            long now = System.nanoTime();
                            long target = TimeUnit.NANOSECONDS.toSeconds(now-started)*rate;
                            while(created<target)
                            {
                                created++;
                                connect.connect(addresses[random.nextInt(addresses.length)]);
                            }
                        }
                        catch(Exception e)
                        {
                            e.printStackTrace();
                        }
                    }
                }).start();
            }

            while(connect.isRunning())
            {
                Thread.sleep(5000);
                System.err.println();
                synchronized (connect)
                {
                    connect.dump = true;
                }                   
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            usage();
        }

    }

    private class Connection
    {
        final SocketChannel channel;
        final long initiated;
        long connected;
        
        Connection() throws Exception
        {
            channel = SocketChannel.open();
            initiated = System.nanoTime();
        }

        public void close()
        {
            try
            {
                if (!isConnected())
                    failures.increment();
                else
                    connections.decrement();
                channel.close();
            }
            catch (IOException e)
            {
                // e.printStackTrace();
            }
        }

        public void connected()
        {
            connected = System.nanoTime();
            long latency = connected-initiated;
            samples.set(latency);
            connections.increment();
        }

        public boolean isConnected()
        {
            return connected!=0;
        }
    }

}
