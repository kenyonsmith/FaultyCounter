using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

// Everyone is responsible for bringing back the process below it
// If a process is called with pIndex=2 but just 2 arguments, it starts pIndex 2
// If a process is called with pIndex=2 but 4 arguments, it doesn't start
// args[0] is pIndex
// args[1] is process ID for process with pIndex=3
// args[2] is process ID for process with pIndex=2
// args[3] is process ID for process with pIndex=1
// args[4] is process ID for process with pIndex=0

namespace FaultyCounter
{
    class Program
    {
        const int MaxPIndex = 4;
        static Process newProc;
        static int pIndex;
        static int childPIndex;
        static int[] processIds;
        static UdpClient recvClient;

        const int MaxCount = 100;
        static int count = 0;
        static int lastCountMS = 0;
        static Mutex countMutex = new Mutex();

        static int sleepTime = 5;
        static bool processingComplete = false;

        const int Opcode_PID = 100;
        const int Opcode_CNT = 200;
        const int Opcode_Celebration = 300;

        static void Main(string[] args)
        {
            // Get pIndex, coutn and lastCountMS
            if (args.Length == 0)
            {
                // No arguments, so we are the parent and we need to have kids
                Console.WriteLine("Hello World! Argc is " + args.Length);
                pIndex = MaxPIndex;
                childPIndex = pIndex - 1;
                count = 0;
                lastCountMS = DateTime.Now.Second * 1000 + DateTime.Now.Millisecond;
            } else if (args.Length >= 3)
            {
                // We were given a pIndex, take note
                Console.WriteLine("Boo! Index is " + args[0] + ", Count is " + args[1]);
                pIndex = Convert.ToInt32(args[0]);
                childPIndex = pIndex - 1 < 0 ? MaxPIndex : pIndex - 1;
                count = Convert.ToInt32(args[1]);
                lastCountMS = Convert.ToInt32(args[2]);
            } else
            {
                System.Environment.Exit(-1);
            }

            // Create the process ID cache
            processIds = new int[MaxPIndex + 1];

            // Initialize process IDs cache from arguments
            for (int i = 0; i <= MaxPIndex; i++)
            {
                if (i < args.Length)
                {
                    // 1st arg is maxPIndex, so goes in last spot in array at position MaxPIndex
                    // this way pIndex=0 goes in spot 0, pIndex=1 goes in spot 1, etc.
                    processIds[MaxPIndex - i] = Convert.ToInt32(args[i + 3]);
                }
                else
                {
                    // Zero-initialize
                    processIds[MaxPIndex - i] = -1;
                }
            }

            // Store pIndex (overwriting arguments) and send process ID
            processIds[pIndex] = Process.GetCurrentProcess().Id;
            if (args.Length != 0)
                SendProcessId();

            // Start updating process IDs as they come in
            recvClient = new UdpClient();
            StartListener();

            // If child process hasn't yet been started, start it
            if (processIds[childPIndex] == -1)
            {
                // Child process is uninitialized, start it
                StartChildProcess();
            }
            else
            {
                // Child process is live, store it
                try
                {
                    newProc = Process.GetProcessById(processIds[childPIndex]);
                }
                catch (ArgumentException)
                {
                    Console.WriteLine("Unable to start, bringing back process " + childPIndex);
                    StartChildProcess();
                }
            }
            ProcessingLoop();
        }

        static void ReceiveMessage(IAsyncResult res)
        {
            IPEndPoint localEp = new IPEndPoint(IPAddress.Any, 1993);
            byte[] result = recvClient.EndReceive(res, ref localEp);
            recvClient.BeginReceive(new AsyncCallback(ReceiveMessage), null);

            int opcode = To_Int16_With_Endianness(result, 0, true);
            if (opcode == Opcode_PID)
            {
                int msg_pIndex = To_Int16_With_Endianness(result, 2, true);
                if (msg_pIndex == pIndex)
                    System.Environment.Exit(0);
                int msg = To_Int16_With_Endianness(result, 4, true);
                Console.WriteLine("Received a message from pIndex " + msg_pIndex + ": Port " + msg);
                if ((msg_pIndex >= 0) && (msg_pIndex <= MaxPIndex))
                {
                    lock (processIds)
                    {
                        processIds[msg_pIndex] = msg;
                    }
                }
            } else if (opcode == Opcode_CNT)
            {
                lock(countMutex)
                {
                    int localCount = To_Int16_With_Endianness(result, 2, true);
                    if (localCount > count)
                    {
                        count = localCount;
                        lastCountMS = To_Int16_With_Endianness(result, 4, true);
                    }
                }
            } else if (opcode == Opcode_Celebration)
            {
                processingComplete = true;
            }
        }

        static void StartListener()
        {
            recvClient.ExclusiveAddressUse = false;

            IPEndPoint localEp = new IPEndPoint(IPAddress.Any, 1993);

            recvClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            recvClient.Client.Bind(localEp);

            IPAddress multicastAddress = IPAddress.Parse("239.0.0.118");
            recvClient.JoinMulticastGroup(multicastAddress);

            recvClient.BeginReceive(new AsyncCallback(ReceiveMessage), null);
        }

        static void ProcessingLoop()
        {
            while (!processingComplete)
            {
                if (newProc != null)
                {
                    newProc.Refresh();
                    if (newProc.HasExited)
                    {
                        Console.WriteLine("Bringing back process " + childPIndex);
                        StartChildProcess();
                    } else
                    {
                        sleepTime = Math.Min(sleepTime * 2, 2000);     // Double sleep time to free up CPU
                    }
                }
                if (pIndex == MaxPIndex)
                {
                    int nowMS = DateTime.Now.Second * 1000 + DateTime.Now.Millisecond;
                    if (Math.Abs(nowMS - lastCountMS) > 1000)
                    {
                        lock (countMutex)
                        {
                            lastCountMS = nowMS;
                            count++;
                            SendCount();

                            if (count >= MaxCount)
                            {
                                StreamWriter writer = new StreamWriter("FinalComputationResult.txt");
                                writer.WriteLine(count);
                                writer.Close();
                                SendCelebration();
                                processingComplete = true;
                            }
                        }
                        Console.WriteLine(count);
                    }

                    Thread.Sleep(Math.Min(sleepTime, 100)); // Parent thread never sleeps more than 100ms
                } else
                {
                    Thread.Sleep(sleepTime);
                }
            }
        }

        static void StartChildProcess()
        {
            newProc = new Process();
            newProc.StartInfo.FileName = "FaultyCounter.exe";

            // Start with one arg for the child's pIndex
            string[] args = new string[3 + MaxPIndex + 1];
            args[0] = Convert.ToString(childPIndex);
            args[1] = Convert.ToString(count);
            args[2] = Convert.ToString(lastCountMS);

            // Add a process ID for each process in our cache, including ourselves
            lock (processIds)
            {
                for (int i = 0; i < MaxPIndex + 1; i++)
                    args[i + 3] = Convert.ToString(processIds[MaxPIndex - i]);
            }
            newProc.StartInfo.Arguments = String.Join(" ", args);

            newProc.Start();
            lock(processIds)
            {
                processIds[childPIndex] = newProc.Id;
            }
        }

        static void SendProcessId()
        {
            byte[] packet = new byte[6];
            // Opcode 100 = Process ID incoming
            Get_Bits_With_Endianness(BitConverter.GetBytes(Opcode_PID), 0, packet, 0, 2, true);
            Get_Bits_With_Endianness(BitConverter.GetBytes(pIndex), 0, packet, 2, 2, true);
            Get_Bits_With_Endianness(BitConverter.GetBytes(processIds[pIndex]), 0, packet, 4, 2, true);

            UdpClient client = new UdpClient();
            IPAddress multicastAddress = IPAddress.Parse("239.0.0.118");
            client.JoinMulticastGroup(multicastAddress);
            IPEndPoint endPoint = new IPEndPoint(multicastAddress, 1993);

            client.Send(packet, 6, endPoint);
        }

        static void SendCount()
        {
            byte[] packet = new byte[6];
            // Opcode 200 = Process ID incoming
            Get_Bits_With_Endianness(BitConverter.GetBytes(Opcode_CNT), 0, packet, 0, 2, true);
            Get_Bits_With_Endianness(BitConverter.GetBytes(count), 0, packet, 2, 2, true);
            Get_Bits_With_Endianness(BitConverter.GetBytes(lastCountMS), 0, packet, 4, 2, true);

            UdpClient client = new UdpClient();
            IPAddress multicastAddress = IPAddress.Parse("239.0.0.118");
            client.JoinMulticastGroup(multicastAddress);
            IPEndPoint endPoint = new IPEndPoint(multicastAddress, 1993);

            client.Send(packet, 6, endPoint);
        }

        static void SendCelebration()
        {
            byte[] packet = new byte[2];
            // Opcode 300 = Celebrate!
            Get_Bits_With_Endianness(BitConverter.GetBytes(Opcode_Celebration), 0, packet, 0, 2, true);

            UdpClient client = new UdpClient();
            IPAddress multicastAddress = IPAddress.Parse("239.0.0.118");
            client.JoinMulticastGroup(multicastAddress);
            IPEndPoint endPoint = new IPEndPoint(multicastAddress, 1993);

            client.Send(packet, 6, endPoint);
        }

        static private UInt16 To_Int16_With_Endianness(byte[] src, int srcLoc, bool bigEndian)
        {
            if (BitConverter.IsLittleEndian == bigEndian)
            {
                byte[] segment = new byte[2];
                Buffer.BlockCopy(src, srcLoc, segment, 0, 2);
                Array.Reverse(segment);
                return BitConverter.ToUInt16(segment, 0);
            }
            return BitConverter.ToUInt16(src, srcLoc);
        }

        static private void Get_Bits_With_Endianness(byte[] src, int srcLoc, byte[] dest, int destLoc, int count, bool bigEndian)
        {
            byte[] segment = new byte[src.Length];
            Buffer.BlockCopy(src, srcLoc, segment, 0, count);
            if (BitConverter.IsLittleEndian == bigEndian)
            {
                // Convert to correct endianness
                Array.Reverse(segment);

            }
            if (BitConverter.IsLittleEndian)
            {
                Buffer.BlockCopy(segment, 2, dest, destLoc, count);
            }
            else
            {
                Buffer.BlockCopy(segment, 0, dest, destLoc, count);
            }
        }
    }
}
