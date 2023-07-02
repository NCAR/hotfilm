
#include <LabJackM.h>

#include <string>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <vector>
#include <exception>
#include <memory>

#include <pthread.h>

#include <nidas/core/Project.h>
#include <nidas/core/DSMConfig.h>
#include <nidas/core/NidasApp.h>
#include <nidas/core/FileSet.h>
#include <nidas/core/DSMEngine.h>
#include <nidas/core/SampleOutputRequestThread.h>
#include <nidas/dynld/SampleOutputStream.h>
#include <nidas/core/DSMSensor.h>
#include <nidas/core/CharacterSensor.h>
#include <nidas/util/Logger.h>
#include <nidas/util/UTime.h>
#include <nidas/util/InvalidParameterException.h>

using std::string;
using std::vector;
using std::cout;
using std::cerr;
using std::endl;
using std::list;

using namespace nidas::core;

using nidas::dynld::SampleOutputStream;
using nidas::util::Logger;
using nidas::util::LogConfig;
using nidas::util::LogContext;
using nidas::util::LogMessage;
using nidas::util::UTime;
using nidas::util::InvalidParameterException;


std::string
ljm_error_to_string(int err, int eaddress=-1)
{
    char errName[LJM_MAX_NAME_SIZE];
    std::ostringstream msg;
    LJM_ErrorToString(err, errName);
    if (err >= LJME_WARNINGS_BEGIN && err <= LJME_WARNINGS_END)
    {
        msg << "warning " << errName;
    }
    else if (err != LJME_NOERROR)
    {
        msg << "error " << errName;
    }
    if (eaddress >= 0)
    {
        msg << " at address " << eaddress;
    }
    return msg.str();
}


void check_error(int err, const std::string& context)
{
    if (err)
    {
        std::ostringstream msg;
        msg << context << ": " << ljm_error_to_string(err);
        PLOG(("") << msg.str());
        throw std::runtime_error(msg.str());
    }
}


const char * NumberToConnectionType(int connectionType)
{
    switch (connectionType) {
    case LJM_ctANY:          return "LJM_ctANY";
    case LJM_ctUSB:          return "LJM_ctUSB";
    case LJM_ctTCP:          return "LJM_ctTCP";
    case LJM_ctETHERNET:     return "LJM_ctETHERNET";
    case LJM_ctWIFI:         return "LJM_ctWIFI";
    case 11:                 return "LJM_ctANY_UDP";
    case LJM_ctNETWORK_UDP:  return "LJM_ctNETWORK_UDP";
    case LJM_ctETHERNET_UDP: return "LJM_ctETHERNET_UDP";
    case LJM_ctWIFI_UDP:     return "LJM_ctWIFI_UDP";
    case LJM_ctNETWORK_ANY:  return "LJM_ctNETWORK_ANY";
    case LJM_ctETHERNET_ANY: return "LJM_ctETHERNET_ANY";
    case LJM_ctWIFI_ANY:     return "LJM_ctWIFI_ANY";
    default:                 return "Unknown connection type";
    }
}

const char * NumberToDeviceType(int deviceType)
{
    switch (deviceType) {
    case LJM_dtANY:     return "LJM_dtANY";
    case LJM_dtT4:      return "LJM_dtT4";
    case LJM_dtT7:      return "LJM_dtT7";
    case 8:             return "LJM_dtT8";
    case LJM_dtTSERIES: return "LJM_dtTSERIES";
    case LJM_dtDIGIT:   return "LJM_dtDIGIT";
    case -4:            return "Demo fake usb";
    default:
        printf(
            "%s:%d NumberToDeviceType: Unknown device type: %d\n",
            __FILE__,
            __LINE__,
            deviceType
        );
        return "Unknown device type";
    }
}


int CountAndOutputNumSkippedScans(int numInChannels, int scansPerRead, double * aData)
{
    int j;
    int numSkippedSamples = 0;
    for (j = 0; j < numInChannels * scansPerRead; j++) {
        if (aData[j] == LJM_DUMMY_VALUE) {
            ++numSkippedSamples;
        }
    }
    if (numSkippedSamples) {
        PLOG(("****** %d data scans were placeholders for scans that were skipped ******",
              numSkippedSamples / numInChannels));
        PLOG(("****** %.01f %% of the scans were skipped ******",
              100 * (double)numSkippedSamples / scansPerRead / numInChannels));
    }
    return numSkippedSamples / numInChannels;
}


void
set_name(int handle, const std::string& name, double value)
{
    ILOG(("setting ") << name << " to " << value);
    int err = LJM_eWriteName(handle, name.c_str(), value);
    if (err)
    {
        std::ostringstream msg;
        msg << "eWriteName(" << handle << ", " << name << ", " << value << ")";
        check_error(err, msg.str());
    }
}


namespace nidas { namespace dynld {
    class LabJackSensor;
}}


/**
 * HotFilm encapsulates the settings to stream hot film anemometer channels
 * from the LabJack T7 ADC and record them to disk.
 */
class HotFilm
{
public:
    // HotFilm();

    // LJM library handle for the T7 device
    int handle = -1;

    // How fast to stream in Hz
    double INIT_SCAN_RATE = 2000;

    int STREAM_TRIGGER_INDEX = 0;
    int STREAM_CLOCK_SOURCE = 0;
    int STREAM_RESOLUTION_INDEX = 8;
    double STREAM_SETTLING_US = 0;
    double AIN_ALL_RANGE = 0;

    // How many scans to get per call to LJM_eStreamRead. INIT_SCAN_RATE/2 is
    // recommended
    int SCANS_PER_READ = INIT_SCAN_RATE / 2;

    // How many times to call LJM_eStreamRead before calling LJM_eStreamStop
    int NUM_READS = 0;

    int NUM_CHANNELS = 4;
    bool ENABLE_PPS_COUNTER = true;

    std::string counter_channel = "DIO0_EF_READ_A";

    std::vector<std::string> ain_channels{
        "AIN0", "AIN2", "AIN4", "AIN6"
    };

    // The channel names that will be scanned.
    std::vector<std::string> channel_names;

    int DeviceType = -1;
    int ConnectionType = -1;
    int SerialNumber = 0;
    int IPAddress = 0;
    int Port = 0;
    int MaxBytesPerMB = 0;
    bool Diagnostics = false;

    // Addresses for the channels.
    vector<int> aScanList;
    vector<int> aScanTypes;

    // data storage
    vector<double> aData;

    nidas::dynld::LabJackSensor* labjack = 0;

    int open();

    int close();

    int get_handle_info();

    int get_channel_addresses();

    void configure_stream();

    void stream();

    std::string device_info();
};



int HotFilm::open()
{
    // Open first found LabJack
    int err = LJM_Open(LJM_dtT7, LJM_ctETHERNET_TCP, "LJM_idANY", &handle);
    check_error(err, "open(T7, USB)");
    if (err)
        return err;

    if ((err = get_handle_info()))
        return err;
    ILOG(("") << device_info());
    return LJME_NOERROR;
}


int HotFilm::close()
{
    int err = LJM_Close(handle);
    handle = -1;
    check_error(err, "close()");
    return err;
}


int HotFilm::get_handle_info()
{
    int err = LJM_GetHandleInfo(handle, &DeviceType, &ConnectionType,
        &SerialNumber, &IPAddress, &Port, &MaxBytesPerMB);
    check_error(err, "GetHandleInfo()");
    return err;
}


std::string
HotFilm::device_info()
{
    char ipAddressString[LJM_IPv4_STRING_SIZE];

    std::ostringstream msg;
    msg << "deviceType: " << NumberToDeviceType(DeviceType);
    msg << "; connectionType: " << NumberToConnectionType(ConnectionType);
    msg << "; serialNumber: " << SerialNumber;

    LJM_NumberToIP(IPAddress, ipAddressString);
    msg << "; IP address: " << ipAddressString;
    msg << "; pipe: " << Port;

    msg << "; max bytes per packet: " << MaxBytesPerMB;
    return msg.str();
}


int
HotFilm::
get_channel_addresses()
{
    // build up the channel scan list and channel addresses
    vector<const char*> names;
    // pps counter is always first if enabled
    channel_names.clear();
    if (ENABLE_PPS_COUNTER)
    {
        channel_names.push_back(counter_channel);
        names.push_back(counter_channel.c_str());
    }
    for (int i = 0; i < NUM_CHANNELS; ++i)
    {
        channel_names.push_back(ain_channels[i]);
        names.push_back(ain_channels[i].c_str());
    }
    unsigned int nchannels = names.size();
    aScanList.resize(nchannels);
    aScanTypes.resize(nchannels);
    int err = LJM_NamesToAddresses(channel_names.size(), names.data(), aScanList.data(), aScanTypes.data());
    check_error(err, "Getting positive channel addresses");
    return err;
}


void
HotFilm::
configure_stream()
{
    ILOG(("Making sure stream is stopped."));
    int err = LJM_eStreamStop(handle);
    if (err)
    {
        PLOG(("stopping stream before configuring: ") << ljm_error_to_string(err));
    }

    get_channel_addresses();

    ILOG(("Writing configurations..."));

    if (STREAM_TRIGGER_INDEX == 0) {
        ILOG(("ensuring triggered stream is disabled:"));
    }
    set_name(handle, "STREAM_TRIGGER_INDEX", STREAM_TRIGGER_INDEX);

    if (STREAM_CLOCK_SOURCE == 0) {
        ILOG(("enabling internally-clocked stream:"));
    }
    set_name(handle, "STREAM_CLOCK_SOURCE", STREAM_CLOCK_SOURCE);

    // Configure the analog inputs' negative channel, range, settling time and
    // resolution.
    // Note: when streaming, negative channels and ranges can be configured for
    // individual analog inputs, but the stream has only one settling time and
    // resolution.

    // default resolution index of 0 means index 8 for T7
    set_name(handle, "STREAM_RESOLUTION_INDEX", STREAM_RESOLUTION_INDEX);
    set_name(handle, "STREAM_SETTLING_US", STREAM_SETTLING_US);
    set_name(handle, "AIN_ALL_RANGE", AIN_ALL_RANGE);
    // disable Extended Features on all AIN
    set_name(handle, "AIN_ALL_EF_INDEX", 0);
    // const int AIN_ALL_NEGATIVE_CH = LJM_GND;
    // Set all AIN to differential.
    const int AIN_ALL_NEGATIVE_CH = 1;
    set_name(handle, "AIN_ALL_NEGATIVE_CH", AIN_ALL_NEGATIVE_CH);

    // I presume there's no harm in configuring an input as a counter even if
    // it's not going to be scanned, but leave it disabled just in case...
    DLOG(("setting up counter on DIO0 (FIO0)..."));
    set_name(handle, "DIO0_EF_ENABLE", 0);
    set_name(handle, "DIO0_EF_INDEX", 8);
    if (ENABLE_PPS_COUNTER)
        set_name(handle, "DIO0_EF_ENABLE", 1);
}


namespace nidas { namespace dynld {

// Inherit CharacterSensor just to avoid having to implement buildIODevice()
// and buildSampleScanner().
class LabJackSensor: public nidas::core::CharacterSensor,
                     public nidas::core::SampleConnectionRequester
{
public:
    LabJackSensor()
    {}

    virtual ~LabJackSensor()
    {}

    void connect(SampleOutput* output) throw() override;

    void disconnect(SampleOutput* output) throw() override;

    void publish_sample(Sample* sample);

    HotFilm hotfilm;

    std::set<SampleOutput*> _outputSet;

    nidas::util::Mutex _outputMutex;
};


}} // namespace nidas { namespace dynld {


using namespace nidas::dynld;

NIDAS_CREATOR_FUNCTION(LabJackSensor);


void
LabJackSensor::publish_sample(Sample* sample)
{
    // send this sample to each of the outputs.  See
    // SampleSourceSupport::distribute() for an explanation of the copy.
    // Basically receive() needs to be able to call disconnect() for itself,
    // which means being able to lock the outputs and erase it's pointer.  As
    // long as the receive() is causing the disconnect, meaning the output
    // pointer is valid when receive() is called, then there should be no
    // problem, because the output pointer is not used again after that.
    _outputMutex.lock();
    auto copies(_outputSet);
    _outputMutex.unlock();
    for (auto* output: copies)
    {
        output->receive(sample);
    }
}


/* implementation of SampleConnectionRequester::connect(SampleOutput*) */
void LabJackSensor::connect(SampleOutput* output) throw()
{
    ILOG(("LabJackSensor: connection from ") << output->getName());
    _outputMutex.lock();
    _outputSet.insert(output);
    _outputMutex.unlock();
}

/*
 * An output wants to disconnect: probably the remote dsm_server went
 * down, or a client disconnected.
 */
void LabJackSensor::disconnect(SampleOutput* output) throw()
{
    _outputMutex.lock();
    _outputSet.erase(output);
    _outputMutex.unlock();
    output->flush();
    try {
        output->close();
    } 
    catch (const IOException& ioe) {
        PLOG(("LabJackSensor: error closing ") << output->getName()
            << ioe.what());
    }

    SampleOutput* orig = output->getOriginal();
    if (output != orig)
        SampleOutputRequestThread::getInstance()->addDeleteRequest(output);

    int delay = orig->getReconnectDelaySecs();
    if (delay < 0) return;
    SampleOutputRequestThread::getInstance()->addConnectRequest(orig,this,delay);
}


#ifdef notdef
// This works to setup a DSMEngine and locate the LabJackSensor, but then the
// sensor instance has to have a way to notify the SensorHandler of new
// samples to read.  Instead, try to mimic what DSMEngine does to setup the
// Project and DSMConfig with all the outputs, but then let the sensor itself
// drive the sample reading and distribution.

int main(int argc, char *argv[])
{
    int res = DSMEngine::main(argc, argv);	// deceptively simple
    ILOG(("dsm exiting, status=") << res);
    return res;
}
#endif


void
setProcessPriority()
{
    // We could use something like nidas Thread::setRealTimeFIFOPriority(),
    // except that only works on Thread instances.  So do the equivalent
    // directly with pthread calls.  We could also use nice() and
    // setpriority(), but I think this is the only way to change the
    // scheduling policy to FIFO.  Change the scheduling before changing the
    // user, in case this is relying on starting up as root to have
    // permissions to set real-time priority.
    sched_param priority{50};
    int policy = SCHED_FIFO;
    int result = pthread_setschedparam(pthread_self(), policy, &priority);
    if (result != 0)
    {
        PLOG(("could not set FIFO sched policy with priority ")
             << priority.sched_priority << ": " << strerror(errno));
    }
    result = pthread_getschedparam(pthread_self(), &policy, &priority);
    if (result != 0)
    {
        PLOG(("could not get thread sched parameters: ")
             << strerror(errno));
    }
    else{
        ILOG(("") << "thread policy=" << policy
                  << ", priority=" << priority.sched_priority);
    }
}


int main(int argc, char const *argv[])
{
    NidasApp app("test_t7");
    NidasAppArg ReadCount("-n,--number", "COUNT",
                          "Stop after COUNT reads, unless 0", "0");
    NidasAppArg Diagnostics("--diag", "",
R"""(Enable LabJack Stream diagnostics.
Data are scanned for skipped values, which are reported if found.
For TCP streams, buffer statistics are queried and reported.)""");

    NidasAppArg DisablePPS("--nopps", "",
        "Do not scan the PPS counter, timestamps will be unsynchronized.");
    NidasAppArg NumChannels("--channels", "N",
                            "Scan first N channels: AIN0, AIN2, AIN4, AIN6.",
                            "4");
    NidasAppArg ResolutionIndex("--resolution", "INDEX",
                                "Set the LabJack resolution INDEX, 0-8", "8");
    NidasAppArg ScanRate("--scanrate", "HZ", "Scan rate in Hz", "2000");

    Logger* logger = Logger::getInstance();
    LogConfig lc("info");
    logger->setScheme(logger->getScheme("default").addConfig(lc));

    LabJackSensor labjack;
    HotFilm& hf = labjack.hotfilm;
    hf.labjack = &labjack;

    try {
        app.XmlHeaderFile.setRequired();
        app.Hostname.setDefault("hotfilm");
        app.enableArguments(DisablePPS | NumChannels | ResolutionIndex |
                            ScanRate |
                            app.XmlHeaderFile | app.Hostname |
                            ReadCount | app.Username | Diagnostics |
                            app.Help | app.Version | app.loggingArgs());
        app.parseArgs(argc, argv);
        if (app.helpRequested())
        {
            std::cout << "Usage: " << argv[0] << " [options] \n";
            std::cout << app.usage();
            return 0;
        }
        app.checkRequiredArguments();
        hf.NUM_READS = ReadCount.asInt();
        hf.STREAM_RESOLUTION_INDEX = ResolutionIndex.asInt();
        hf.ENABLE_PPS_COUNTER = !DisablePPS.asBool();
        hf.NUM_CHANNELS = NumChannels.asInt();
        hf.INIT_SCAN_RATE = ScanRate.asFloat();
        hf.SCANS_PER_READ = hf.INIT_SCAN_RATE / 2;

        ILOG(("") << "nchannels=" << hf.NUM_CHANNELS
                  << ", resolution=" << hf.STREAM_RESOLUTION_INDEX
                  << ", scanrate=" << hf.INIT_SCAN_RATE
                  << ", scans_per_read=" << hf.SCANS_PER_READ
                  << ", pps=" << (hf.ENABLE_PPS_COUNTER ? "on" : "off"));
    }
    catch (NidasAppException& appx)
    {
        std::cerr << appx.toString() << std::endl;
        return 1;
    }

    setProcessPriority();
    app.setupProcess();

    // May as well load a project xml to get the project-specific info for the
    // header.
    std::unique_ptr<Project, void(*)(Project*)>
        project(Project::getInstance(), [](Project* p){
            Project::destroyInstance();
        });

    try {
        std::string xmlpath = app.xmlHeaderFile();
        project->parseXMLConfigFile(xmlpath);
        auto pos = xmlpath.find_last_of('/');
        if (pos != string::npos)
        {
            xmlpath = xmlpath.substr(pos+1);
        }
        project->setConfigName(xmlpath);
    }
    catch (InvalidParameterException& xpe)
    {
        std::cerr << xpe.toString() << std::endl;
        return 1;
    }

    std::string hostname = app.getHostName();
    DSMConfig* dsmconfig = project->findDSMFromHostname(hostname);
    if (!dsmconfig) 
    {
        throw InvalidParameterException("dsm", "no match for hostname",
                                        hostname);
    }

    SampleOutputRequestThread::getInstance()->start();
    // Taken from DSMEngine::connectOutputs()
    const list<SampleOutput*>& outputs = dsmconfig->getOutputs();
    list<SampleOutput*>::const_iterator oi;
    for (oi = outputs.begin(); oi != outputs.end(); ++oi) {
        SampleOutput* output = *oi;
        DLOG(("requesting connection from SampleOutput ")
              << "'" << output->getName() << "'");
        SampleOutputRequestThread::getInstance()->addConnectRequest(output, &labjack, 0);
    }

    try {
        hf.open();
        hf.configure_stream();
        hf.stream();
        hf.close();
    }
    catch (std::runtime_error& re)
    {
        return 1;
    }

    return 0;
}


void
HotFilm::
stream()
{
    int err, iteration;
    unsigned int channel;
    int numSkippedScans = 0;
    int totalSkippedScans = 0;
    int deviceScanBacklog = 0;
    int LJMScanBacklog = 0;
    unsigned int receiveBufferBytesSize = 0;
    unsigned int receiveBufferBytesBacklog = 0;

    unsigned int numChannels = channel_names.size();
    unsigned int aDataSize = numChannels * SCANS_PER_READ;
    aData.resize(aDataSize);

    auto scanRate = INIT_SCAN_RATE;
    auto numReads = NUM_READS;
    auto scansPerRead = SCANS_PER_READ;

    ILOG(("Starting stream, %d scans per read, %d channels, "
          "requesting scan rate %.02f...",
          scansPerRead, numChannels, scanRate));
    err = LJM_eStreamStart(handle, scansPerRead, numChannels, aScanList.data(),
                           &scanRate);
    check_error(err, "LJM_eStreamStart");
    ILOG(("Stream started. Actual scan rate: %.02f Hz (%.02f sample rate)",
          scanRate, scanRate * numChannels));

    // Technically scan rate is a double and does not need to divide evenly
    // into a second.  So use the scans per read to compute the samples per
    // second, knowing that it was chosen as half the scan rate.
    unsigned int samples_per_second = 2 * scansPerRead;

    // Create Samples to hold the stats and the channels.  Unlike the data
    // from the labjack which stores by channel first and then by scan, and
    // may not include a full second of scans, we want the sample to contain
    // contiguous full second for each channel.  I haven't seen the returned
    // scan rate be different from the requested, but I suppose technically we
    // should not expect more samples per second than that.

    // keep track of how many scans in each sample so far
    unsigned int nscans_in_sample = 0;

    int DSMID = 200;
    int SENSORID = 500;

    SampleT<float> series[numChannels];
    for (auto& sample: series)
    {
        // Each series 
        unsigned int i = &sample - series;
        // we could try to pull these from the sample tag from the xml, but
        // for now just hardcode it.
        sample.setDSMId(DSMID);
        sample.setSpSId(SENSORID + ((i == 0) ? 2 : 20 + i - 1));
        sample.allocateData(samples_per_second);
        sample.setDataLength(samples_per_second);
    }

    SampleT<float> pps_stats;
    pps_stats.setDSMId(DSMID);
    pps_stats.setSpSId(SENSORID + 1);
    pps_stats.allocateData(6);
    pps_stats.setDataLength(6);

    SampleT<float> stats[numChannels-1];
    for (auto& sample: stats)
    {
        // 3 variables each: avg/min/max
        unsigned int i = &sample - stats;
        sample.setDSMId(DSMID);
        sample.setSpSId(SENSORID + 10 + i);
        sample.allocateData(3);
        sample.setDataLength(3);
    }

    // Somewhere we need to decide what timestamp to assign to a sample before
    // writing it out.  It could be the current time rounded to the second, if
    // the labjack sampling is triggered on the PPS.  However, it seems best
    // not to have to rely on the PPS to trigger sampling, just in case a GPS
    // is not sync'd or goes bad.  If instead we rely on a counter input to
    // detect the leading edge of the PPS, then we can line up the samples
    // with the scan where the counter changes, or else guess.
    //
    // The convention will be that the sample timestamp is for the beginning
    // of the time period covered by the scans.

    // This also implies that the synchronization status will be an important
    // diagnostic, such as the current value of the PPS counter, and a check
    // that the counter is changing every <scanrate> scans.

    dsm_time_t timestamp = 0;
    // the last pps counter value seen:
    int pps_count = -1;
    int pps_step = -1;
    double backlog_pct = 0;
    dsm_time_t timestamp_to_after{0};
    for (iteration = 0; numReads == 0 || iteration < numReads; iteration++)
    {
        // Get a timestamp before the read and after to get stats on how long
        // it takes.  Assume the time after corresponds most closely to the
        // time of the last scan, meaning we get the data back as soon as
        // possible after a scan is complete.  So if the pps counter has a
        // transition in this iteration, then we can assign a timestamp to
        // that transition using the last even second before after.
        dsm_time_t before = nidas::util::getSystemTime();
        err = LJM_eStreamRead(handle, aData.data(), &deviceScanBacklog,
            &LJMScanBacklog);
        dsm_time_t after = nidas::util::getSystemTime();
        float read_time_ms = (after - before) / USECS_PER_MSEC;
        DLOG(("LJM_eStreamRead: ") << "completed in "
             << read_time_ms << " ms");
        check_error(err, "LJM_eStreamRead");

        if (Diagnostics && ConnectionType != LJM_ctUSB)
        {
            err = LJM_GetStreamTCPReceiveBufferStatus(handle,
                &receiveBufferBytesSize, &receiveBufferBytesBacklog);
            check_error(err, "LJM_GetStreamTCPReceiveBufferStatus");
            backlog_pct = receiveBufferBytesBacklog;
            backlog_pct = backlog_pct / receiveBufferBytesSize * 100;
            DLOG(("iteration: %d - deviceScanBacklog: %d, LJMScanBacklog: %d",
                  iteration, deviceScanBacklog, LJMScanBacklog)
                  << "-> receive backlog: " << std::setprecision(0)
                  << backlog_pct << "% of buffer size "
                  << receiveBufferBytesSize << " bytes");
        }
        else
        {
            DLOG(("iteration: %d - deviceScanBacklog: %d, LJMScanBacklog: %d",
                  iteration, deviceScanBacklog, LJMScanBacklog));
        }
        static LogContext lp(LOG_DEBUG);
        if (lp.active())
        {
            for (channel = 0; channel < numChannels; channel++) {
                LogMessage msg(&lp);
                msg << channel_names[channel] << "=" << std::setprecision(3);
                int scan = 0;
                for (; scan < 10 && scan < scansPerRead; ++scan)
                    msg << " " << aData[channel + scan*numChannels];
                
                if (scan + 10 < scansPerRead)
                {
                    scan = scansPerRead - 10;
                    msg << "...";
                }
                for (; scan < scansPerRead; ++scan)
                    msg << " " << aData[channel + scan*numChannels];
            }
        }

        if (Diagnostics)
        {
            numSkippedScans = CountAndOutputNumSkippedScans(numChannels,
                scansPerRead, aData.data());

            if (numSkippedScans) {
                PLOG(("  %d skipped scans in this LJM_eStreamRead",
                    numSkippedScans));
                totalSkippedScans += numSkippedScans;
            }
        }

        // Fill the sample for each channel.
        for (unsigned int channel = 0; channel < numChannels; ++channel)
        {
            float* sdp = series[channel].getDataPtr();
            sdp += nscans_in_sample;
            double* idp = aData.data() + channel;
            for (int scan = 0; scan < scansPerRead; ++scan)
            {
                if (channel == 0 && ENABLE_PPS_COUNTER)
                {
                    // look for a pps counter change.
                    if (pps_count == -1)
                        pps_count = *idp;
                    else if (pps_count != *idp)
                    {
                        pps_step = scan + nscans_in_sample;
                        // Now work backwards to get the timestamp.  We assume
                        // this only happens every other read.
                        timestamp = (after / USECS_PER_SEC) * USECS_PER_SEC;
                        timestamp -= (1.0 / scanRate) * pps_step * USECS_PER_SEC;
                        DLOG(("pps count transition from ") << pps_count << " to "
                             << *idp << " at scan " << pps_step
                             << ", timestamp adjusted to "
                             << UTime(timestamp).format(true, "%H:%M:%S.%4f"));
                        pps_count = *idp;
                        timestamp_to_after = after - timestamp;
                    }
                }
                *(sdp++) = *idp;
                idp += numChannels;
            }
        }
        nscans_in_sample += scansPerRead;

        // if this is full, compute the means and write it out.
        if (nscans_in_sample == samples_per_second)
        {
            // if timestamp has not been set, because there have been no pps
            // transitions, or else if it has not changed since the last
            // sample, then use after minus one second.
            if (!timestamp || pps_step == -1)
            {
                PLOG(("") << "no pps step detected in last second, "
                             "approximating time tag");
                timestamp = after - USECS_PER_SEC;
                timestamp_to_after = after - timestamp;
            }
            else
            {
                // check the difference between the last sample and the time
                // tag to be used for this next sample; expect it to be close
                // to 1.0 seconds. If instead it's close to 0 or 2, then
                // assume the wrong system time was truncated, and adjust by 1
                // second.  Otherwise use what was calculated for the given
                // step.
                dsm_time_t diff = timestamp - pps_stats.getTimeTag();
                dsm_time_t adjust = 0;
                // expect a really tight fit for being off by one second in
                // either direction, no more than one scan, otherwise
                // something else could be wrong.
                const dsm_time_t threshold = 500 /*microseconds*/;
                if (abs(diff) <= threshold)
                {
                    adjust += USECS_PER_SEC;
                }
                else if (abs(diff - 2*USECS_PER_SEC) < threshold)
                {
                    adjust -= USECS_PER_SEC;
                }
                if (adjust)
                {
                    timestamp += adjust;
                    timestamp_to_after -= adjust;
                    PLOG(("") << "pps step detected but timestamp is off by "
                              << diff << "usecs, "
                              << "adjusted towards expected value: "
                              << UTime(timestamp).format(true, "%H:%M:%S.%4f"));
                }
                // either the timestamp is now close to the expected value, or
                // else it was significantly off and is being used as is.  I
                // suppose we could use the read time (after-before) to
                // further inform this algorithm, since we have some idea that
                // timing is off when the reads are not taking about 500 ms...
            }
            pps_stats.setTimeTag(timestamp);

            // no stats sample for the pps counter first in scan list
            unsigned int channel = 0;
            if (ENABLE_PPS_COUNTER)
                series[channel++].setTimeTag(timestamp);
            for ( ; channel < numChannels; ++channel)
            {
                float min{0}, max{0};
                series[channel].setTimeTag(timestamp);
                float* sdp = series[channel].getDataPtr();
                double sum = 0;
                for (unsigned int scan = 0; scan < samples_per_second; ++scan)
                {
                    min = (scan == 0) ? *sdp : std::min(min, *sdp);
                    max = (scan == 0) ? *sdp : std::max(max, *sdp);
                    sum += *(sdp++);
                }
                float mean = sum/samples_per_second;
                stats[channel-1].setTimeTag(timestamp);
                float* vars = stats[channel-1].getDataPtr();
                vars[0] = mean;
                vars[1] = min;
                vars[2] = max;
            }
            float* pps_vars = pps_stats.getDataPtr();
            pps_vars[0] = pps_count;
            pps_vars[1] = pps_step;
            pps_vars[2] = deviceScanBacklog;
            pps_vars[3] = LJMScanBacklog;
            pps_vars[4] = read_time_ms;
            pps_vars[5] = timestamp_to_after;
            static LogContext lp(LOG_DEBUG);
            if (lp.active())
            {
                LogMessage msg(&lp, "stats:");
                msg << std::setprecision(3);
                for (unsigned int i = 0; i < numChannels-1; ++i)
                {
                    float* vars = stats[i].getDataPtr();
                    msg << " " << channel_names[i+1] << ":"
                        << vars[0] << "/" << vars[1] << "/" << vars[2];
                }
            }
            if (labjack)
            {
                // could decide not to publish the pps_stats if pps counter is
                // disabled, but leave it for now as a reminder that the
                // timestamps are not sync'd.
                labjack->publish_sample(&pps_stats);
                for (auto& sample: series)
                {
                    labjack->publish_sample(&sample);
                }
                for (auto& sample: stats)
                {
                    labjack->publish_sample(&sample);
                }
            }
            nscans_in_sample = 0;
            // reset pps step index to make sure it is not used again in the
            // next samples, it has to be set on the next pps count change.
            pps_step = -1;
            timestamp_to_after = 0;
        }
    }
    if (totalSkippedScans) {
        PLOG(("****** Total number of skipped scans: %d ******",
              totalSkippedScans));
    }

    ILOG(("Stopping stream"));
    err = LJM_eStreamStop(handle);
    check_error(err, "Stopping stream");
}
