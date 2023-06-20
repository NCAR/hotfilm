
#include <LabJackM.h>

#include <string>
#include <sstream>
#include <iostream>
#include <vector>
#include <exception>
#include <memory>

#include <nidas/core/Project.h>
#include <nidas/core/NidasApp.h>
#include <nidas/core/FileSet.h>
#include <nidas/dynld/SampleOutputStream.h>
#include <nidas/util/Logger.h>
#include <nidas/util/UTime.h>
#include <nidas/util/InvalidParameterException.h>

using std::string;
using std::vector;

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
        printf("****** %d data scans were placeholders for scans that were skipped ******\n",
            numSkippedSamples / numInChannels);
        printf("****** %.01f %% of the scans were skipped ******\n",
            100 * (double)numSkippedSamples / scansPerRead / numInChannels);
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

    // How many scans to get per call to LJM_eStreamRead. INIT_SCAN_RATE/2 is
    // recommended
    int SCANS_PER_READ = INIT_SCAN_RATE / 2;

    // How many times to call LJM_eStreamRead before calling LJM_eStreamStop
    int NUM_READS = 10;

    // Channels/Addresses to stream.
    std::vector<std::string> channel_names{"AIN0", "AIN1", "AIN2", "AIN3"};

    int DeviceType = -1;
    int ConnectionType = -1;
    int SerialNumber = 0;
    int IPAddress = 0;
    int Port = 0;
    int MaxBytesPerMB = 0;

    // Addresses for the channels.
    vector<int> aScanList;
    vector<int> aScanTypes;

    // data storage
    vector<double> aData;

    nidas::dynld::SampleOutputStream* outstream = 0;

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
    int err = LJM_Open(LJM_dtT7, LJM_ctUSB, "LJM_idANY", &handle);
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
    // cache the addresses for the channel names
    unsigned int nchannels = channel_names.size();
    const char* names[nchannels];
    for (unsigned int i = 0; i < nchannels; ++i)
        names[i] = channel_names[i].c_str();
    aScanList.resize(nchannels);
    aScanTypes.resize(nchannels);
    int err = LJM_NamesToAddresses(channel_names.size(), names, aScanList.data(), aScanTypes.data());
    check_error(err, "Getting positive channel addresses");
    return err;
}


void
HotFilm::
configure_stream()
{
    const int STREAM_TRIGGER_INDEX = 0;
    const int STREAM_CLOCK_SOURCE = 0;
    const int STREAM_RESOLUTION_INDEX = 0;
    const double STREAM_SETTLING_US = 0;
    const double AIN_ALL_RANGE = 0;
    const int AIN_ALL_NEGATIVE_CH = LJM_GND;

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
    set_name(handle, "STREAM_RESOLUTION_INDEX", STREAM_RESOLUTION_INDEX);
    set_name(handle, "STREAM_SETTLING_US", STREAM_SETTLING_US);
    set_name(handle, "AIN_ALL_RANGE", AIN_ALL_RANGE);
    set_name(handle, "AIN_ALL_NEGATIVE_CH", AIN_ALL_NEGATIVE_CH);
}


int main(int argc, char const *argv[])
{
    NidasApp app("test_t7");
    Logger* logger = Logger::getInstance();
    LogConfig lc("info");
    logger->setScheme(logger->getScheme("default").addConfig(lc));

    try {
        app.XmlHeaderFile.setRequired();
        app.Hostname.setRequired();
        app.enableArguments(app.XmlHeaderFile | app.OutputFiles | app.Hostname | \
                            app.Help | app.Version | app.loggingArgs());
        app.parseArgs(argc, argv);
        if (app.helpRequested())
        {
            std::cout << "Usage: " << argv[0] << " [options] \n";
            std::cout << app.usage();
            return 0;
        }
        app.checkRequiredArguments();
    }
    catch (NidasAppException& appx)
    {
        std::cerr << appx.toString() << std::endl;
        return 1;
    }

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

    FileSet* outSet = 0;
    std::unique_ptr<SampleOutputStream> outStream;
    if (app.OutputFiles.specified())
    {
        outSet = new FileSet();
        outSet->setFileName(app.outputFileName());
        outSet->setFileLengthSecs(app.outputFileLength());
        outStream.reset(new SampleOutputStream(outSet));
    }

    HotFilm hf;

    try {
        hf.open();
        hf.configure_stream();
        hf.outstream = outStream.get();
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

    // Create a Sample to hold the channels.  Unlike the data from the labjack
    // which stores by channel first and then by scan, and may not include a
    // full second of scans, we want the sample to contain contiguous full
    // seconds for each channel.  I haven't seen the returned scan rate be
    // different from the requested, but I suppose technically we should not
    // expect more samples per second than that.

    // For now, assume the sample layout as follows:
    //
    // Sample id 1 is the means:
    //
    // channel 0 1-second mean double, ... , channel N-1 1-second mean double
    //
    // Sample id 2 is the full 2 KHz samples:
    //
    // channel 0 scan-rate doubles, ... , channel N scan-rate doubles
    //
    // At some point we'll have to manufacture a SampleTag for that.

    unsigned int doubles_per_sample = samples_per_second * numChannels;

    SampleT<double> sample;
    sample.allocateData(doubles_per_sample);
    sample.setDataLength(doubles_per_sample);
    unsigned int nscans_in_sample = 0;

    SampleT<double> means;
    means.allocateData(numChannels);
    means.setDataLength(numChannels);

    // Here's where we would set the sample id from the xml.
    means.setDSMId(200);
    means.setSpSId(501);
    sample.setDSMId(200);
    sample.setSpSId(502);

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

    // Read the scans
    ILOG(("Now performing %d reads", numReads));
    for (iteration = 0; iteration < numReads; iteration++) {
        err = LJM_eStreamRead(handle, aData.data(), &deviceScanBacklog,
            &LJMScanBacklog);
        check_error(err, "LJM_eStreamRead");

        ILOG(("iteration: %d - deviceScanBacklog: %d, LJMScanBacklog: %d",
              iteration, deviceScanBacklog, LJMScanBacklog));
        if (ConnectionType != LJM_ctUSB) {
            err = LJM_GetStreamTCPReceiveBufferStatus(handle,
                &receiveBufferBytesSize, &receiveBufferBytesBacklog);
            check_error(err, "LJM_GetStreamTCPReceiveBufferStatus");
            ILOG(("-> receive backlog: %f%%",
                 ((double)receiveBufferBytesBacklog) / receiveBufferBytesSize * 100));
        }
        printf("\n");
        printf("  1st scan out of %d:\n", scansPerRead);
        for (channel = 0; channel < numChannels; channel++) {
            printf("    %s = %0.5f\n", channel_names[channel].c_str(), aData[channel]);
        }

        numSkippedScans = CountAndOutputNumSkippedScans(numChannels,
            scansPerRead, aData.data());

        if (numSkippedScans) {
            printf("  %d skipped scans in this LJM_eStreamRead\n",
                numSkippedScans);
            totalSkippedScans += numSkippedScans;
        }
        printf("\n");

        // Fill the sample one channel at a time.
        for (unsigned int channel = 0; channel < numChannels; ++channel)
        {
            double* sdp = sample.getDataPtr();
            sdp += (channel * samples_per_second);
            sdp += nscans_in_sample;
            double* idp = aData.data() + channel;
            for (int scan = 0; scan < scansPerRead; ++scan)
            {
                *(sdp++) = *idp;
                idp += numChannels;
            }
        }
        nscans_in_sample += scansPerRead;

        // if this is full, compute the means and write it out.
        if (nscans_in_sample == samples_per_second)
        {
            // proxy for timestamp, now - 1 second.
            sample.setTimeTag(nidas::util::getSystemTime() - USECS_PER_SEC);
            means.setTimeTag(sample.getTimeTag());
            for (unsigned int channel = 0; channel < numChannels; ++channel)
            {
                double* sdp = sample.getDataPtr();
                // skip the means.
                sdp += numChannels;
                sdp += (channel * samples_per_second);
                double sum = 0;
                for (unsigned int scan = 0; scan < samples_per_second; ++scan)
                {
                    sum += *sdp;
                }
                means.getDataPtr()[channel] = sum/samples_per_second;
            }
            static LogContext lp(LOG_DEBUG);
            if (lp.active())
            {
                LogMessage msg(&lp, "sample full, computed means:");
                for (unsigned int i = 0; i < numChannels; ++i)
                {
                    msg << " " << means.getDataPtr()[i];
                }
            }
            if (outstream)
            {
                outstream->receive(&means);
                outstream->receive(&sample);
            }
            nscans_in_sample = 0;
        }
    }
    if (totalSkippedScans) {
        printf("\n****** Total number of skipped scans: %d ******\n\n",
            totalSkippedScans);
    }

    ILOG(("Stopping stream"));
    err = LJM_eStreamStop(handle);
    check_error(err, "Stopping stream");
}
