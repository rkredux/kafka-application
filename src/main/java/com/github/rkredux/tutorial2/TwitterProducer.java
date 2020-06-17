package com.github.rkredux.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.awt.*;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {
    //create all the sub classes and methods off of it
    //use the run to trigger an execution from main function

    //function named same as the parent class does not need a return type
    //also called as a constructor function
    //a constructor function runs when you use the keyword new to create
    //an object of instance class
    public TwitterProducer(){

    }


    public static void main(String[] args) {
        TwitterProducer twitterProducer = new TwitterProducer();
        twitterProducer.run();
    }

    public void run(){
        //  create Twitter client
        //  get the tweets from Twitter
        //  Loop over the tweets and ship them to a topic in Kafka
    }

    public void createTwitterClient(){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        //BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        ArrayList<Long> followings = Lists.newArrayList(1234L, 566788L);
        ArrayList<String> terms = Lists.newArrayList("twitter", "api");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1("consumerKey", "consumerSecret", "token", "secret");



    }


}
