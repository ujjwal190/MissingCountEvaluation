/*
Once the Mapper phase is completed(100%),JobTracker will initiate the sort & shuffle phase.
Once sort & shuffle is completed,JobTracker will initiate the new phase by picking the business 
logic from the reducer code from the Jar file 

*/

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

class MissingCountReducer extends Reducer<Text, IntWritable, Text, Text>
{
  private int counterSum = 0 ;
  private float total = 0 ;
  private float percent ;
  private Text result = new Text();
  private Text column ;
  private Text values ;
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
  {
    counterSum = 0 ; 
    total = 0 ;
    for(IntWritable value : values)
    {
        counterSum += value.get();
        total++ ;
    }
    
    percent = (float)(counterSum * 100 / total) ;
    
    StringBuilder b = new StringBuilder();
    b.append(counterSum);
    b.append('\t');
    b.append(percent);    

    result.set(b.toString());

    context.write(key, result);
  }
//@ override
//For the setup of the Output headers
  public void run(Context context) throws IOException, InterruptedException 
  {
        setup(context);
        column = new Text("ColumnName") ;
        values = new Text("MissingCount" + "\t" + "Percentage") ;
        context.write(column, values); 
        try 
        {
          while (context.nextKey()) 
          {
            reduce(context.getCurrentKey(), context.getValues(), context);
            // If a back up store is used, reset it
            Iterator<IntWritable> iter = context.getValues().iterator();
            if(iter instanceof ReduceContext.ValueIterator) 
            {              
            	((ReduceContext.ValueIterator<IntWritable>)iter).resetBackupStore();        
            }
          }
        } 
        finally 
        {
          cleanup(context);
        }
  }
}