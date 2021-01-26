#Anomaly Score

###Data description:
The dataset contains the daily network download size (in MB) for five people for 17 days. The first column contains the name of individual involved in network download activity. The other columns contain the daily download size for the 17 days.

###Problem:
Identification of an anomalous person while performing any download activity. The program should calculate a daily anomaly score for each user based on their previous download sizes. The candidate should write the custom algorithm to calculate the daily anomaly score and use this module in spark framework with Python as the language. Please do not use the well-known libraries such as scikit, pandas etc. (* Statistical significance test not required because of smaller dataset.)


###Output:
Produce a file as the same format of input data replacing the network download bytes by daily anomaly score. The output data should allow us to measure the numeric quantity of the anomaly value.

## Two solutions proposed here:

### First solution: Only previous days

I made two different solutions, because from the e-mail
I understood: calculate a score of a date based on the previous dates. That would mean that my first day
wouldn't have a score because I don't have a previous day do compare to.
I chose to evaluate the anomaly score using the z score, 
this means I would need at least two days to start calculating scores.
Following this line of thinking
I have the first day with score 0. Then I apply the logic of calculating the z score for the remaining days taking in context only the current + previous days

### Second solution: Overall anomaly score
For this solution I calculate the overall mean and standard deviation and I apply to every column.
In my humble opinion, this approach calculates a better overall anomaly score
that's why I got a little confused

## meaning of z score:

z(x) = (x - mean) / std for x in row(user)

in addition if I only have two days for the first solution, I only the mean and std of those two days.

Values away from 0 (positive or negative) mean that they are not close to the mean
and therefore are more anomalous than others.