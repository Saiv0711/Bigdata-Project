/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package youtube;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] row = value.toString().split("\t");
        //goal1
        //context.write(new Text(row[3]), new IntWritable(Integer.parseInt(row[7])));
        
        //goal2
        //context.write(new Text(row[3]), new IntWritable(Integer.parseInt(row[6])));
        
        //goal3
        //context.write(new Text(row[3]), new IntWritable(Integer.parseInt(row[9])));
        
        //goal4
        //context.write(new Text(row[3]), new IntWritable(Integer.parseInt(row[7])));
        
        //goal5
       //context.write(new Text(row[3]), new IntWritable(Integer.parseInt(row[8])));
        
        //goal6
        // context.write(new Text(row[3]), new IntWritable(Integer.parseInt(row[6])));
        
        //goal7
        //context.write(new Text(row[3]), new IntWritable(Integer.parseInt(row[7])));
        
        //goal8
        //context.write(new Text(row[3]+" "+row[1]), new IntWritable(Integer.parseInt(row[7])));
        
        //goal9
        //context.write(new Text(row[1]), new IntWritable(Integer.parseInt(row[6])));
        
        //goal10
        
       context.write(new Text(row[1]+" "+row[6]), new IntWritable(Integer.parseInt(row[6])));
    }
    
}
