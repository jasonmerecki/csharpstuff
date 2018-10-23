using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace jasonmerecki.AffinityStrategy
{
	public enum AffinityStrategy
	{
		HASH, ROUNDROBIN
	}

	/*
	 * Executor based on thread affinity for the passed non-null object key, such that
	 * all tasks associated with a given key are guaranteed to execute in the same thread,
	 * in the order in which they were submit to the AffinityProcessor. One relaxed exception:
	 * if two tasks for the same key are submit concurrently, order is not guaranteed.
	 * The object is fully usable immediately after construction, and cannot be used
	 * upon Shutdown.
	 */
	public class AffinityProcessor 
	{
		private readonly AffinityStrategy strategy;
		private readonly int queueCapacity; // size of the BlockingCollection queue (not yet implemented)
		private int round = -1; // used for round-robin strategy
		public int Round { get { return this.round;} } // exposed only for unit tests
		private ConcurrentDictionary<object, Int32> keyMapping = new ConcurrentDictionary<object, Int32>(); // map of object key to int index
		private readonly SingleExecutor[] emptyExecutors = new SingleExecutor[0];
		private volatile SingleExecutor[] executors; // the executors
		private static readonly ThreadStart thatGirl = () => { }; // because it is poison

		public AffinityProcessor(AffinityStrategy strategy, int threadCapacity)
		{
			this.strategy = strategy;
			SingleExecutor[] myExecs = new SingleExecutor[threadCapacity];
			for (int i = 0; i < threadCapacity; i++)
			{
				SingleExecutor exec = new SingleExecutor(i);
				// it is important to ensure all of the consumer executor threads
				// have started before exiting the constructor, otherwise this 
				// object may not yet be ready to accept ThreadStart work items
				exec.StartEvent.WaitOne();
				myExecs[i] = exec;
			}
			executors = myExecs;
		}

		// shuts down this processor, optionally waiting for millsWaitToComplete milliseconds
		// for already submit tasks to complete. Incomplete tasks at the end of the wait
		// are lost. If millsWaitToComplete is zero, then waits indefinitely, and if
		// millsWaitToComplete is negative then there is no waiting.  The chosen pattern for
		// shutdown is a poison pill when waiting for tasks, because the author concluded that
		// is the least complicated way for the calling thread to wait for tasks but immediately
		// return when tasks are complete.
		public List<ThreadStart> Shutdown(int millsWaitToComplete)
		{
			List<ThreadStart> unexecuted = new List<ThreadStart>();
			// prevent multiple threads from shutdown
			lock (this)
			{
				if (this.executors == emptyExecutors)
				{
					// already shutdown
					return unexecuted;
				}
				SingleExecutor[] myExecs = this.executors;
				this.executors = emptyExecutors; // prevents more work from being submit
				
				DateTime then = DateTime.Now;
				int remainMills = millsWaitToComplete;
				bool allDied = true;
				// shut down the existing executors (perhaps check on unfinished work?)
				foreach (SingleExecutor exec in myExecs)
				{
					if (millsWaitToComplete > 0)
					{
						exec.ExecuteWork(thatGirl);
						bool didJoin = exec.Runner.Join(remainMills);
						int elapsed = (int)(DateTime.Now - then).TotalMilliseconds;

						if (!didJoin)
						{
							// there is a bit of a race here, the executor may complete 
							// after the join exits but before we reach here. The cancellation
							// may be complete, therefore this is done in a try/catch to protect the caller.
							try
							{
								BlockingCollection<ThreadStart> thisUnexecuted = exec.CancelRemaining();
								unexecuted.AddRange(thisUnexecuted);
							}
							catch (System.ObjectDisposedException)
							{
								// we can just eat this
							}
						}

						remainMills = Math.Max(0, remainMills - elapsed);
						allDied = didJoin && allDied;
					}
					else if (millsWaitToComplete == 0)
					{
						exec.ExecuteWork(thatGirl);
						exec.Runner.Join();
					}
					else
					{
						try
						{
							// cancel immediately
							BlockingCollection<ThreadStart> thisUnexecuted = exec.CancelRemaining();
							// why no add range in Collection?
							unexecuted.AddRange(thisUnexecuted);
						}
						catch (System.ObjectDisposedException)
						{
							// should not expect this, but just in case
						}
					}
				}
				
			}
			return unexecuted;
		}

		public bool SubmitWork(object key, ThreadStart work)
		{
			// if caller passes a null key or work
			if (key == null)
			{
				throw new ArgumentNullException("Cannot execute work for null key");
			}
			if (work == null)
			{
				throw new ArgumentNullException($"Cannot execute null work for key {key}");
			}
			SingleExecutor[] myExecs = this.executors;
			if (myExecs != null && myExecs.Length != 0)
			{
				int idx = -1;
				int execLength = myExecs.Length;
				while (!keyMapping.TryGetValue(key, out idx))
				{
					switch (strategy)
					{
						case AffinityStrategy.ROUNDROBIN:
							// round robin requires a lock to prevent multiple threads
							// for the same key from incrementing the idx value (could not be rolled back properly)
							lock (this)
							{
								if (!keyMapping.TryGetValue(key, out idx))
								{
									// winning thread executes this
									round = (round + 1) >= execLength ? 0 : (round + 1);
									keyMapping.TryAdd(key, round);
								}
							}
							idx = round;
							break;
						case AffinityStrategy.HASH:
							idx = Math.Abs(key.GetHashCode() % execLength);
							// don't need to know if successful, the top of the loop will get the idx
							keyMapping.TryAdd(key, idx); 
							break;
					}
				}
				SingleExecutor exec = myExecs[idx];
				bool willexec = exec.ExecuteWork(work);
				return willexec;
			} 
			return false; // work is submit after shutdown or before object is fully initialized
		}


		/*
		 * Contains a single BlockingCollection and Thread, to execute tasks
		 */
		private class SingleExecutor
		{
			private readonly BlockingCollection<ThreadStart> works = new BlockingCollection<ThreadStart>();
			private readonly CancellationTokenSource cts = new CancellationTokenSource();
			private readonly int index;
			private readonly Thread runner;
			public Thread Runner
			{
				get { return runner; }
			}
			private readonly AutoResetEvent startEvent = new AutoResetEvent(false);
			public AutoResetEvent StartEvent
			{
				get { return startEvent; }
			}

			public SingleExecutor(int index)
			{
				this.index = index;
				runner = new Thread(() =>
				{
					try
					{
						while (!cts.IsCancellationRequested)
						{
							ThreadStart nextWork = works.Take(cts.Token);
							
							if (nextWork == thatGirl)
							{
								break;
							}
							try
							{
								nextWork?.Invoke();
							}
							catch (Exception ex)
							{
								// best effort to log and/or alert
								
							}
						}
					}
					catch (System.OperationCanceledException opCance)
					{
						Console.WriteLine($"Cancellation, assume shutdown, executor index {index}");
					}
					finally
					{
						if (cts != null)
						{
							cts.Dispose();
						}
					}
				});
				runner.Start();
				this.startEvent.Set();
			}

			public bool ExecuteWork(ThreadStart work)
			{
				if ((!cts.IsCancellationRequested))
				{
					works.Add(work);
					return true;
				}
				return false;
			}

			public BlockingCollection<ThreadStart> CancelRemaining()
			{
				cts.Cancel();
				return works;
			}
			
		}

	}


}
