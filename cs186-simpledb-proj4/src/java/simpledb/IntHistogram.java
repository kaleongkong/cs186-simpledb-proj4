package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
	protected int max;
	protected int min;
	protected int buckets;
	protected int total;
	public int[] histogram;
	protected int width;
    public IntHistogram(int buckets, int min, int max) {
    	this.max = max;
    	this.min = min;
    	this.buckets = buckets;
    	this.total = 0;
    	this.histogram = new int[buckets];
    	this.width = (int)Math.ceil((double)(max - min +1)/(double)buckets);
    	
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	double pos = position(v);
    	//System.out.println("pos: "+pos);
    	if(pos>=0 && pos<buckets){
    		histogram[(int)pos]++;
    	}
    	total++;
    }
    public double position(int v){
    	int num = max - min +1; //subtraction is always give me 1 number less than the number of int from min to max
    	double portion =((double)(v-min)/(double)num);
    	//System.out.println("portion: "+portion);
    	double pos = (portion*buckets);
    	return pos;
    }
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double equalpredicate(int v){
    	return (double)(histogram[(int)position(v)]/(double)width)/(double)total;
    }
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
    	if(op.equals(op.EQUALS)||op.equals(op.LIKE)){
    		if(v<min || v>max){
    			return 0;
    		}
    		return equalpredicate(v);
    	}else if(op.equals(op.GREATER_THAN)){
    		if(v<min){
    			return 1.0;
    		}
    		if(v>=max){
    			return 0;
    		}
    		int pos= (int) position(v);
    		int totallargerthanv = 0;
    		double w_b = (double)(max-min+1)/(double)buckets;
    		double bright = w_b*(pos+1)+min;
    		double b_part = (bright-v)/w_b;
    		for(int i =pos+1; i<buckets; i++){
    			if(pos>=0 && pos<buckets){
    				totallargerthanv+= histogram[i];
    			}
    		}
    		return (b_part*histogram[pos]+(double)totallargerthanv)/(double)total;
    	}else if(op.equals(op.GREATER_THAN_OR_EQ)){
    		if(v<=min){
    			return 1.0;
    		}
    		if(v>max){
    			return 0;
    		}
    		int pos= (int) position(v);
    		int totallargerthanv = 0;
    		double w_b = (double)(max-min+1)/(double)buckets;
    		double bright = w_b*(pos+1)+min;
    		double b_part = (bright-v)/w_b;
    		for(int i =pos+1; i<buckets; i++){
    			if(pos>=0 && pos<buckets){
    				totallargerthanv+= histogram[i];
    			}
    		}
    		return equalpredicate(v)+((b_part*histogram[pos]+(double)totallargerthanv)/(double)total);
    	}else if(op.equals(op.LESS_THAN)){
    		if(v<=min){
    			return 0;
    		}
    		if(v>max){
    			return 1;
    		}
    		int pos = (int) position(v);
    		int totallessthanv = 0;
    		double w_b = (double)(max-min+1)/(double)buckets;
    		double bleft = w_b*(pos)+min;
    		double b_part = (v-bleft)/w_b;
    		for(int i= 0; i< pos; i++){
    			totallessthanv+= histogram[i];
    		}
    		return (b_part*histogram[pos]+(double)totallessthanv)/(double)total;
    	}else if(op.equals(op.LESS_THAN_OR_EQ)){
    		if(v<min){
    			return 0;
    		}
    		if(v>=max){
    			return 1;
    		}
    		int pos = (int) position(v);
    		int totallessthanv = 0;
    		double w_b = (double)(max-min+1)/(double)buckets;
    		double bleft = w_b*(pos)+min;
    		double b_part = (v-bleft)/w_b;
    		for(int i= 0; i< pos; i++){
    			totallessthanv+= histogram[i];
    		}
    		return equalpredicate(v)+((b_part*histogram[pos]+(double)totallessthanv)/(double)total);
    	}else if(op.equals(op.NOT_EQUALS)){
    		if(v<min || v>max){
    			return 1;
    		}
    		
    		return 1.0-equalpredicate(v);
    	}
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
    /*
    public static void main(String[] args){
    	IntHistogram h = new IntHistogram(10,1,10);
    	h.addValue(3);
    	h.addValue(3);
    	h.addValue(3);
    	h.addValue(1);
    	h.addValue(10);
    	for(int i=0; i<h.histogram.length; i++){
    		System.out.println(h.histogram[i]);
    	}
    }*/
}
