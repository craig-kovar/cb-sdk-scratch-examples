using Couchbase.Core.Retry;
using Couchbase.Core.Exceptions;

/*
Example Retry Strategy that uses the best effort strategy except 
when a QueryPreparedStatementFailure is detected.  In this case,  it will 
throw an IndexNotFoundException so you can capture this and prepare query
*/
public class MyRetry: IRetryStrategy
{

    private readonly IBackoffCalculator _backoffCalculator;

    /*
        Default constructor that will set the backoff calculator
        to an Exponential Backoff Calculator with a maximum number
        of retries set to 10,  starting delay of 1 millisecond and max
        delay of 500 milliseconds
    */
    public MyRetry() :
        this(ExponentialBackoff.Create(10, 1, 500))
    {
    }

    /*
        Constructor that allows you to provide your own Backoff Calculator
    */
    public MyRetry(IBackoffCalculator calculator)
    {
        _backoffCalculator = calculator;
    }


    /*
        Retry logic
    */
    public RetryAction RetryAfter(IRequest request, RetryReason reason) {

        // Detect the reason is QueryPreparedStatementFailure 
        // (i.e. named prepared statement is missing )
        if (reason == RetryReason.QueryPreparedStatementFailure)
        {
            //Throw an exception we can catch on and re-prepare
            throw new IndexNotFoundException();
        }

        //If the request is retriable, calculate the back off duration and retry
        if (request.Idempotent || reason.AllowsNonIdempotentRetries())
            {
                var backoffDuration = _backoffCalculator.CalculateBackoff(request);
                return RetryAction.Duration(backoffDuration);
            }

        //If the request is not retriable 
        return RetryAction.Duration(null);
    }

}