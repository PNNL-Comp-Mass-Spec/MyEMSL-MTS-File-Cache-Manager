using System;

namespace MyEMSL_MTS_File_Cache_Manager
{
	public static class clsWindowsUpdateStatus
	{

		/// <summary>
		/// Checks whether Windows Updates are expected to occur close to the current time of day
		/// </summary>
		/// <param name="pendingWindowsUpdateMessage">Output: description of the pending or recent Windows updates</param>
		/// <returns></returns>
		/// <remarks></remarks>
		public static bool UpdatesArePending(out string pendingWindowsUpdateMessage)
		{
			return UpdatesArePending(DateTime.Now, out pendingWindowsUpdateMessage);
		}

		/// <summary>
		/// Checks whether Windows Updates are expected to occur close to the current time of day
		/// </summary>
		/// <param name="currentTime">Current time of day</param>
		/// <param name="pendingWindowsUpdateMessage">Output: description of the pending or recent Windows updates</param>
		/// <returns></returns>
		/// <remarks></remarks>
		public static bool UpdatesArePending(DateTime currentTime, out string pendingWindowsUpdateMessage)
		{

			pendingWindowsUpdateMessage = "No pending update";

			// Determine the second Tuesday in the current month
			var firstTuesdayInMonth = new DateTime(currentTime.Year, currentTime.Month, 1);
			while (firstTuesdayInMonth.DayOfWeek != DayOfWeek.Tuesday)
			{
				firstTuesdayInMonth = firstTuesdayInMonth.AddDays(1);
			}

			var secondTuesdayInMonth = firstTuesdayInMonth.AddDays(7);

			// Windows 7 / Windows 8 Pubs install updates around 3 am on the Thursday after the second Tuesday of the month
			// Do not request a job between 12 am and 6 am on Thursday in the week with the second Tuesday of the month
			var dtExclusionStart = secondTuesdayInMonth.AddDays(2);
			var dtExclusionEnd = secondTuesdayInMonth.AddDays(2).AddHours(6);

			if (currentTime >= dtExclusionStart && currentTime < dtExclusionEnd)
			{
				var dtPendingUpdateTime = secondTuesdayInMonth.AddDays(2).AddHours(3);

				if (currentTime < dtPendingUpdateTime)
				{
					pendingWindowsUpdateMessage = "Processing boxes are expected to install Windows updates around " + dtPendingUpdateTime.ToString("hh:mm:ss tt");
				}
				else
				{
					pendingWindowsUpdateMessage = "Processing boxes should have installed Windows updates at " + dtPendingUpdateTime.ToString("hh:mm:ss tt");
				}

				return true;
			}

			// Windows servers install updates around either 3 am or 10 am on the Sunday after the second Tuesday of the month
			// Do not request a job between 2 am and 4 am or between 9 am and 11 am on Sunday in the week with the second Tuesday of the month
			dtExclusionStart = secondTuesdayInMonth.AddDays(5).AddHours(2);
			dtExclusionEnd = secondTuesdayInMonth.AddDays(5).AddHours(4);

			var dtExclusionStart2 = secondTuesdayInMonth.AddDays(5).AddHours(9);
			var dtExclusionEnd2 = secondTuesdayInMonth.AddDays(5).AddHours(11);


			if ((currentTime >= dtExclusionStart && currentTime < dtExclusionEnd) || (currentTime >= dtExclusionStart2 && currentTime < dtExclusionEnd2))
			{
				var dtPendingUpdateTime1 = secondTuesdayInMonth.AddDays(5).AddHours(3);
				var dtPendingUpdateTime2 = secondTuesdayInMonth.AddDays(5).AddHours(10);

				var pendingUpdateTimeText = dtPendingUpdateTime1.ToString("hh:mm:ss tt") + " or " + dtPendingUpdateTime2.ToString("hh:mm:ss tt");

				if (currentTime < dtPendingUpdateTime2)
				{
					pendingWindowsUpdateMessage = "Servers are expected to install Windows updates around " + pendingUpdateTimeText;
				}
				else
				{
					pendingWindowsUpdateMessage = "Servers should have installed Windows updates around " + pendingUpdateTimeText;
				}

				return true;
			}

			return false;

		}
	}
}
