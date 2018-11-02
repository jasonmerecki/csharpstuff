using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace jasonmerecki.AffinityStrategy
{
	[TestClass]
	public class AffinityProcessorTests1
	{

		[TestMethod]
		public void TestAffinityProcessorBasic()
		{
			foreach (AffinityStrategy strat in Enum.GetValues(typeof(AffinityStrategy)))
			{
				AffinityProcessor ap = new AffinityProcessor(strat, 3);
				ConcurrentQueue<WorkUnitInfo> outputWorks = new ConcurrentQueue<WorkUnitInfo>();
				ThreadStart player1 = (new WorkUnit(outputWorks, "Doug Glatt", 0)).DoWork;
				ap.SubmitWork("hockey", player1);
				ap.Shutdown(0); // wait indefinitely 
				Assert.AreEqual(1, outputWorks.Count, "Not enough work units in the list.");
				Console.WriteLine("Done with test!");
			}
		}

		[TestMethod]
		public void TestAffinityProcessorKeyIndex()
		{
			foreach (AffinityStrategy strat in Enum.GetValues(typeof(AffinityStrategy)))
			{
				AffinityProcessor ap = new AffinityProcessor(strat, 3);
				ConcurrentQueue<WorkUnitInfo> outputWorks = new ConcurrentQueue<WorkUnitInfo>();
				ThreadStart player1 = (new WorkUnit(outputWorks, "Doug Glatt", 0)).DoWork;
				ap.SubmitWork("hockey", player1);
				ThreadStart player2 = (new WorkUnit(outputWorks, "Marco Belchior", 0)).DoWork;
				ap.SubmitWork("hockey", player2);
				ThreadStart player3 = (new WorkUnit(outputWorks, "Ross Rhea", 0)).DoWork;
				ap.SubmitWork("hockey", player3);
				ThreadStart player4 = (new WorkUnit(outputWorks, "Xavier Laflamme", 0)).DoWork;
				ap.SubmitWork("hockey", player4);
				ThreadStart player5 = (new WorkUnit(outputWorks, "John Biebe", 0)).DoWork;
				ap.SubmitWork("hockey", player5);
				ThreadStart player6 = (new WorkUnit(outputWorks, "'Skank' Marden", 0)).DoWork;
				ap.SubmitWork("hockey", player6);
				ap.Shutdown(0); // wait indefinitely 
				Assert.AreEqual(6, outputWorks.Count, "Not enough work units in the list.");
				int mainThreadId = 0;
				foreach (WorkUnitInfo info in outputWorks)
				{
					mainThreadId = (mainThreadId == 0) ? info.ThreadId : mainThreadId;
					Assert.AreEqual(mainThreadId, info.ThreadId, "Did not reuse threads for the same key.");
				}
				Console.WriteLine("Done with test!");
			}
		}

		[TestMethod]
		public void TestAffinityProcessorRoundRobin()
		{
			// tests if the tasks are assigned round-robin to the executors
			AffinityProcessor ap = new AffinityProcessor(AffinityStrategy.ROUNDROBIN, 3);
			ConcurrentQueue<WorkUnitInfo> outputWorks = new ConcurrentQueue<WorkUnitInfo>();
			ThreadStart player1 = (new WorkUnit(outputWorks, "Doug Glatt", 0)).DoWork;
			ap.SubmitWork("hockey", player1);
			ThreadStart player2 = (new WorkUnit(outputWorks, "Ricky 'Wild Thing' Vaughn", 0)).DoWork;
			ap.SubmitWork("baseball", player2);
			ThreadStart player3 = (new WorkUnit(outputWorks, "Rod Tidwell", 0)).DoWork;
			ap.SubmitWork("football", player3);
			Assert.AreEqual(2, ap.Round, "Did not round robin and count the right way.");
			ThreadStart player4 = (new WorkUnit(outputWorks, "Happy Gilmore", 0)).DoWork;
			ap.SubmitWork("golf", player4);
			ap.Shutdown(0); // wait indefinitely 
			Assert.AreEqual(4, outputWorks.Count, "Not enough work units in the list.");
			Assert.AreEqual(0, ap.Round, "Did not round robin and wrap the right way.");
			int threadRound1 = 0;
			foreach (WorkUnitInfo info in outputWorks)
			{
				if (info.workOutput == "Doug Glatt" || info.workOutput == "Happy Gilmore")
				{
					threadRound1 = (threadRound1 == 0) ? info.ThreadId : threadRound1;
					Assert.AreEqual(threadRound1, info.ThreadId, "Did not reuse threads for round robin.");
				}
			}
			Console.WriteLine("Done with test!");
		}

		[TestMethod]
		public void TestAffinityProcessorWaiting()
		{
			// longer tasks, to test shutdown and waiting for all tasks
			AffinityProcessor ap = new AffinityProcessor(AffinityStrategy.ROUNDROBIN, 3);
			ConcurrentQueue<WorkUnitInfo> outputWorks = new ConcurrentQueue<WorkUnitInfo>();
			ThreadStart player1 = (new WorkUnit(outputWorks, "Doug Glatt", 20)).DoWork;
			ap.SubmitWork("hockey", player1);
			ThreadStart player2 = (new WorkUnit(outputWorks, "Ricky 'Wild Thing' Vaughn", 20)).DoWork;
			ap.SubmitWork("baseball", player2);
			ThreadStart player3 = (new WorkUnit(outputWorks, "Rod Tidwell", 20)).DoWork;
			ap.SubmitWork("football", player3);
			ap.Shutdown(30); // should complete by this time, everything run in parallel
			Assert.AreEqual(3, outputWorks.Count, "Not enough work units in the list.");
			Console.WriteLine("Done with test!");
		}

		[TestMethod]
		public void TestAffinityProcessorNotWaiting()
		{
			// longer tasks, to test shutdown and waiting for all tasks
			AffinityProcessor ap = new AffinityProcessor(AffinityStrategy.ROUNDROBIN, 3);
			ConcurrentQueue<WorkUnitInfo> outputWorks = new ConcurrentQueue<WorkUnitInfo>();
			ThreadStart player1 = (new WorkUnit(outputWorks, "Doug Glatt", 20)).DoWork;
			ap.SubmitWork("hockey", player1);
			ThreadStart player2 = (new WorkUnit(outputWorks, "Ricky 'Wild Thing' Vaughn", 20)).DoWork;
			ap.SubmitWork("baseball", player2);
			ThreadStart player3 = (new WorkUnit(outputWorks, "Rod Tidwell", 20)).DoWork;
			ap.SubmitWork("football", player3);
			ap.Shutdown(5); // should exit waiting before the work units have added themselves
			Assert.AreEqual(0, outputWorks.Count, "Did not exit waiting for shutdown correctly.");
			Console.WriteLine("Done with test!");
		}

		[TestMethod]
		public void TestAffinityProcessorCancel()
		{
			// longer tasks, to test shutdown and waiting for all tasks
			AffinityProcessor ap = new AffinityProcessor(AffinityStrategy.ROUNDROBIN, 3);
			ConcurrentQueue<WorkUnitInfo> outputWorks = new ConcurrentQueue<WorkUnitInfo>();
			ThreadStart player1 = (new WorkUnit(outputWorks, "Doug Glatt", 20)).DoWork;
			ap.SubmitWork("hockey", player1);
			ThreadStart player2 = (new WorkUnit(outputWorks, "Ricky 'Wild Thing' Vaughn", 20)).DoWork;
			ap.SubmitWork("baseball", player2);
			ThreadStart player3 = (new WorkUnit(outputWorks, "Rod Tidwell", 20)).DoWork;
			ap.SubmitWork("football", player3);
			ap.Shutdown(-1); // cancel immediately
			Assert.AreEqual(0, outputWorks.Count, "Did not exit waiting for shutdown correctly.");
			Console.WriteLine("Done with test!");
		}

		[TestMethod]
		public void TestAffinityProcessorCancelWithRemaining()
		{
			// longer tasks, to test shutdown and waiting for all tasks
			AffinityProcessor ap = new AffinityProcessor(AffinityStrategy.ROUNDROBIN, 3);
			ConcurrentQueue<WorkUnitInfo> outputWorks = new ConcurrentQueue<WorkUnitInfo>();
			ThreadStart player1 = (new WorkUnit(outputWorks, "Doug Glatt", 20)).DoWork;
			ap.SubmitWork("hockey", player1);
			ThreadStart player2 = (new WorkUnit(outputWorks, "Marco Belchior", 20)).DoWork;
			ap.SubmitWork("hockey", player2);
			ThreadStart player3 = (new WorkUnit(outputWorks, "Ross Rhea", 20)).DoWork;
			ap.SubmitWork("hockey", player3);
			ThreadStart player4 = (new WorkUnit(outputWorks, "Xavier Laflamme", 20)).DoWork;
			ap.SubmitWork("hockey", player4);
			Thread.Sleep(5); // give one work task a chance to start executing
			List<ThreadStart> unexecuted = ap.Shutdown(-1); // cancel immediately and get remaining three tasks
			Assert.AreEqual(3, unexecuted.Count, "Did not cancel and get unexecuted tasks correctly.");
			Assert.AreEqual(0, outputWorks.Count, "Did not exit waiting for shutdown correctly.");
			Console.WriteLine("Done with test!");
		}

	    [TestMethod]
	    public void TestAffinityProcessorSendParams()
	    {
	        foreach (AffinityStrategy strat in Enum.GetValues(typeof(AffinityStrategy)))
	        {
	            AffinityProcessor ap = new AffinityProcessor(strat, 3);
	            ConcurrentQueue<WorkUnitInfo> outputWorks = new ConcurrentQueue<WorkUnitInfo>();
	            WorkUnit workUnit = new WorkUnit(outputWorks, "Doug Glatt", 0);
	            ThreadStart player1 = () => { workUnit.DoWorkWith("Steve Yzerman"); };
	            ap.SubmitWork("hockey", player1);
	            ap.Shutdown(0); // wait indefinitely 
	            Assert.AreEqual(1, outputWorks.Count, "Not enough work units in the list.");
	            WorkUnitInfo info;
	            outputWorks.TryDequeue(out info);
	            Assert.IsTrue(info.workOutput.Equals("Steve Yzerman"), "Didn't find the right player!");
	            Console.WriteLine("Done with test!");
	        }
	    }

        // encapsulates some information about the work done inside the affinity processor
        private class WorkUnitInfo
		{
			private int threadId = Thread.CurrentThread.GetHashCode();
			public int ThreadId
			{
				get { return threadId;}
			}
			public string workOutput { get; set; }
		}

		private class WorkUnit
		{
			private ConcurrentQueue<WorkUnitInfo> outputWorks = new ConcurrentQueue<WorkUnitInfo>();
			private readonly string workOutput;
			private readonly int maxWaitTime;
			public WorkUnit(ConcurrentQueue<WorkUnitInfo> outputWorks, String workOutput, int maxWaitTime)
			{
				this.outputWorks = outputWorks;
				this.workOutput = workOutput;
				this.maxWaitTime = maxWaitTime;
			}

		    public void DoWork()
		    {
		        DoWorkWith(this.workOutput);
		    }
		    public void DoWorkWith(string workOutput)
		    {
		        WorkUnitInfo i = new WorkUnitInfo();
		        i.workOutput = workOutput;
		        Console.WriteLine(workOutput);
		        if (maxWaitTime > 0)
		        {
		            // todo: make this a random time
		            Thread.Sleep(maxWaitTime);
		        }
		        outputWorks.Enqueue(i);
		    }
        }

		
	}
}
