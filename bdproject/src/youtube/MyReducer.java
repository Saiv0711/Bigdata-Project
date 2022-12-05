/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package youtube;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MyReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
//    	//goal 1
//    	 int count = 0;
//        int totallikes = 0;
//       for(IntWritable value: values){
//          totallikes += value.get();
////        	count += 1;
//        	
//                       		
//        }
//       double avglikes = totallikes/count;
//        context.write(key, new DoubleWritable(avglikes));
//               
//    }
    	
    	//goal2 
//    	int count = 0;
//      int totalviews = 0;
//     for(IntWritable value: values){
//        totalviews += value.get();
////      	count += 1;
//      	
//                     		
//      }
//     double avgviews = totalviews/count;
//      context.write(key, new DoubleWritable(avgviews));
//             
//  }
    	
    	//goal3
    	
//int count = 0;
//        
//        for(IntWritable value: values){
//        	count += value.get();
//        	
//                       		
//        }
//       
//        context.write(key, new DoubleWritable(count));
//               
//    }

    //goal4
//int count = 0;
//        
//        for(IntWritable value: values){
//        	count += value.get();
//        	
//                       		
//        }
//       
//        context.write(key, new DoubleWritable(count));
//               
//    }
    	
    	//goal5
//int count = 0;
//        
//        for(IntWritable value: values){
//        	count += value.get();
//        	
//                       		
//        }
//       
//        context.write(key, new DoubleWritable(count));
//               
//    }
    	//goal 6,7,8,9, 10
        int count = 0;
        
        for(IntWritable value: values){
        	count += value.get();
        	
                       		
        }
       
        context.write(key, new DoubleWritable(count));
               
    }
}
