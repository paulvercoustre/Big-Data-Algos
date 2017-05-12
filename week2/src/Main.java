import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class Main {

	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		System.out.println("Salut");
		
		Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/twitter", "root", "password");
		//Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/twitter?useUnicode=yes&characterEncoding=UTF8&" + "user=root&password=");
		
		ConfigurationBuilder cb = new ConfigurationBuilder(); 
		cb.setDebugEnabled(true).setOAuthConsumerKey("bwbYJzfjMy6GVg6BoEA4WdTH0") // basXIvROD3J1BSAvWOWwxm2Wi
		.setOAuthConsumerSecret("bzaT5PHXiIBSFjPX4Lo6iuk4F6HzA0ZdjrqJXwlcjfXtlsDJuM") // C1mApGdB2ZDbQzNZ7Sfsj4N6vX5CSPBXGtqAsLh6Q55TlQA0CE
		.setOAuthAccessToken("15614869-J2RqJgqndaDvwZK7J7z1b2FCBqk4a5ZHLM3RdDjF7") //109226669-77QGoI2Cqoir71GWJKRlq8RJEAqRC628FARdJ897
		.setOAuthAccessTokenSecret("CysHYd0xmMhsn8BPYp4cwIUabhyBWcOWCDR0ZnoHcPxtl"); //0I6DRWcmbz2oF76Socmg6OedRUoar6u0FcD9MtJaceK0j
		
		TwitterStream twittFactory = new TwitterStreamFactory(cb.build()).getInstance(); 
		TweetListener2 list = new TweetListener2(conn); 
		twittFactory.addListener(list);
		twittFactory.sample();
		
	}

}
