using System;

namespace MyEMSL_MTS_File_Cache_Manager
{
    [Obsolete("Use clsEventNotifier in PRISM.dll")]
    public delegate void MessageEventHandler(object sender, MessageEventArgs e);

    [Obsolete("Use clsEventNotifier in PRISM.dll")]
    public delegate void ProgressEventHandler(object sender, ProgressEventArgs e);

    [Obsolete("Use clsEventNotifier in PRISM.dll")]
    public class MessageEventArgs : EventArgs
    {
        public readonly string Message;

        public MessageEventArgs(string message)
        {
            Message = message;
        }
    }

    [Obsolete("Use clsEventNotifier in PRISM.dll")]
    public class ProgressEventArgs : EventArgs
    {
        /// <summary>
        /// Value between 0 and 100
        /// </summary>
        public readonly double PercentComplete;

        public ProgressEventArgs(double percentComplete)
        {
            PercentComplete = percentComplete;
        }
    }
}
