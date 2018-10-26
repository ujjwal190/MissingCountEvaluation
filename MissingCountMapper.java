/*
This is the mapReduce code consisting of the Business logic for the mapper code.This business logic is
from the jar file is taken by the JobTracker to initiate the mapper phase on all the available 
TaskTracker in the cluster. 

*/

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MissingCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
  private int ctr1 = 0 ;
  private String [] header;
  private Text columnName = new Text();
  private Text columnHeader ;
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
    String [] columnFields = value.toString().split("\t");
    ctr1 = 0 ;
    for (String field : columnFields)
    {   
       if (field.isEmpty())
       {
           columnName.set(header[ctr1]);
           context.write(columnName, new IntWritable(1));
       }
       else
       {
           columnName.set(header[ctr1]);
           context.write(columnName, new IntWritable(0));         
       }  
       ctr1++ ;
    }
  }
 //@ override
 //For the extraction of the Header Column_name
 //convert the 1st input to string and parse it into the Column
  public void run(Context context) throws IOException, InterruptedException
  {        
        setup(context); 
        try 
        {
          if (context.nextKeyValue())
          {	
              columnHeader = context.getCurrentValue();
              header =  columnHeader.toString().split("\t");
              for (String column:header)
            	  System.out.println(column) ; 
          } 	
          while (context.nextKeyValue()) 
          {
            map(context.getCurrentKey(), context.getCurrentValue(), context);
          }
        } 
        // finally 
        // {
          // cleanup(context);
        // }      
  }
}