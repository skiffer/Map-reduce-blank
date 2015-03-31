# Advices

##### Debugging tips
- Use 'provided' scope for deps
- Write unit tests
- First test on locally 
- Finally test on cluster
- When throw an exception then task will be failed (if it fails max attempt times)
- Dont use logging for mapreduce that will be to handle huge amount of data
    - Logging only critical data in catch block
    
##### Developing strategy and testing strategies
0. Testing with LocalJobRunner
    - [minus] Doest support distibuted cache
    - [minus] ony one reducer
    - use MRUnit framework for testing
1. Develop in locally (one local JVM), with small data
2. Test on PSEUDO-distributed mode ( several demon processes), with small data
3. Test on FULLY-distributed mode, with small data
4. Test on FULLY-distributed mode, with big amount data



 