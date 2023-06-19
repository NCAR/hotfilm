
#include <LabJackM.h>

#include <string>
#include <sstream>
#include <iostream>
#include <vector>
#include <exception>


#include <nidas/core/NidasApp.h>
#include <nidas/util/Logger.h>


using std::string;
using std::vector;

using namespace nidas::core;
using namespace nidas::util;


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


std::string
device_info_string(int handle)
{
    int DeviceType, ConnectionType, SerialNumber, IPAddress, Port,
        MaxBytesPerMB;

    std::ostringstream msg;

    int err = LJM_GetHandleInfo(handle, &DeviceType, &ConnectionType,
        &SerialNumber, &IPAddress, &Port, &MaxBytesPerMB);

    if (err)
    {
        PLOG(("GetHandleInfo(") << handle << ") failed: "
              << ljm_error_to_string(err));
        return "";
    }

    char ipAddressString[LJM_IPv4_STRING_SIZE];

    // Print
    msg << "deviceType: " << NumberToDeviceType(DeviceType);
    msg << "; connectionType: " << NumberToConnectionType(ConnectionType);
    msg << "; serialNumber: " << SerialNumber;

    LJM_NumberToIP(IPAddress, ipAddressString);
    msg << "; IP address: " << ipAddressString;
    msg << "; pipe: " << Port;

    msg << "; max bytes per packet: " << MaxBytesPerMB;
    return msg.str();
}





void Stream(int handle, int numChannels, const char ** channelNames,
    double scanRate, int scansPerRead, int numReads);

void HardcodedConfigureStream(int handle);

NidasApp app("test_t7");


int main(int argc, char const *argv[])
{
    Logger* logger = Logger::getInstance();
    LogConfig lc("info");
    logger->setScheme(logger->getScheme("default").addConfig(lc));

    app.enableArguments(app.Help | app.loggingArgs());
    app.parseArgs(argc, argv);
    if (app.helpRequested())
    {
        std::cout << "Usage: " << argv[0] << " [options] \n";
        std::cout << app.usage();
        return 0;
    }

    // How fast to stream in Hz
    double INIT_SCAN_RATE = 2000;

    // How many scans to get per call to LJM_eStreamRead. INIT_SCAN_RATE/2 is
    // recommended
    int SCANS_PER_READ = INIT_SCAN_RATE / 2;

    // How many times to call LJM_eStreamRead before calling LJM_eStreamStop
    const int NUM_READS = 10;

    // Channels/Addresses to stream. NUM_CHANNELS can be less than or equal to
    // the size of CHANNEL_NAMES
    enum { NUM_CHANNELS = 2 };
    const char * CHANNEL_NAMES[] = {"AIN0", "AIN1"};

    // Open first found LabJack
    int handle, err;
    err = LJM_Open(LJM_dtT7, LJM_ctUSB, "LJM_idANY", &handle);
    if (err)
    {
        std::cerr << "open(T7, USB) failed: " << ljm_error_to_string(err)
                  << std::endl;
        return 1;
    }

    ILOG(("") << device_info_string(handle));

    Stream(handle, NUM_CHANNELS, CHANNEL_NAMES, INIT_SCAN_RATE, SCANS_PER_READ,
        NUM_READS);

    err = LJM_Close(handle);
    if (err)
        PLOG(("") << "Close(" << handle << ") failed: " << ljm_error_to_string(err));

    return 0;
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


void HardcodedConfigureStream(int handle)
{
    const int STREAM_TRIGGER_INDEX = 0;
    const int STREAM_CLOCK_SOURCE = 0;
    const int STREAM_RESOLUTION_INDEX = 0;
    const double STREAM_SETTLING_US = 0;
    const double AIN_ALL_RANGE = 0;
    const int AIN_ALL_NEGATIVE_CH = LJM_GND;

    ILOG(("Writing configurations..."));

    if (STREAM_TRIGGER_INDEX == 0) {
        ILOG(("    Ensuring triggered stream is disabled:"));
    }
    set_name(handle, "STREAM_TRIGGER_INDEX", STREAM_TRIGGER_INDEX);

    if (STREAM_CLOCK_SOURCE == 0) {
        ILOG(("    Enabling internally-clocked stream:"));
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

void Stream(int handle, int numChannels, const char ** channelNames,
    double scanRate, int scansPerRead, int numReads)
{
    int err, iteration, channel;
    int numSkippedScans = 0;
    int totalSkippedScans = 0;
    int deviceScanBacklog = 0;
    int LJMScanBacklog = 0;
    unsigned int receiveBufferBytesSize = 0;
    unsigned int receiveBufferBytesBacklog = 0;
    int connectionType;

    vector<int> aScanList(numChannels, 0);

    unsigned int aDataSize = numChannels * scansPerRead;
    vector<double> aData(aDataSize, 0);

    err = LJM_GetHandleInfo(handle, NULL, &connectionType, NULL, NULL, NULL, NULL);
    check_error(err, "GetHandleInfo: ");

    err = LJM_NamesToAddresses(numChannels, channelNames, aScanList.data(), NULL);
    check_error(err, "Getting positive channel addresses");

    HardcodedConfigureStream(handle);

    printf("\n");
    printf("Starting stream...\n");
    err = LJM_eStreamStart(handle, scansPerRead, numChannels, aScanList.data(),
        &scanRate);
    check_error(err, "LJM_eStreamStart");
    printf("Stream started. Actual scan rate: %.02f Hz (%.02f sample rate)\n",
        scanRate, scanRate * numChannels);
    printf("\n");

    // Read the scans
    printf("Now performing %d reads\n", numReads);
    printf("\n");
    for (iteration = 0; iteration < numReads; iteration++) {
        err = LJM_eStreamRead(handle, aData.data(), &deviceScanBacklog,
            &LJMScanBacklog);
        check_error(err, "LJM_eStreamRead");

        printf("iteration: %d - deviceScanBacklog: %d, LJMScanBacklog: %d",
            iteration, deviceScanBacklog, LJMScanBacklog);
        if (connectionType != LJM_ctUSB) {
            err = LJM_GetStreamTCPReceiveBufferStatus(handle,
                &receiveBufferBytesSize, &receiveBufferBytesBacklog);
            check_error(err, "LJM_GetStreamTCPReceiveBufferStatus");
            printf(", receive backlog: %f%%",
                ((double)receiveBufferBytesBacklog) / receiveBufferBytesSize * 100);
        }
        printf("\n");
        printf("  1st scan out of %d:\n", scansPerRead);
        for (channel = 0; channel < numChannels; channel++) {
            printf("    %s = %0.5f\n", channelNames[channel], aData[channel]);
        }

        numSkippedScans = CountAndOutputNumSkippedScans(numChannels,
            scansPerRead, aData.data());

        if (numSkippedScans) {
            printf("  %d skipped scans in this LJM_eStreamRead\n",
                numSkippedScans);
            totalSkippedScans += numSkippedScans;
        }
        printf("\n");
    }
    if (totalSkippedScans) {
        printf("\n****** Total number of skipped scans: %d ******\n\n",
            totalSkippedScans);
    }

    printf("Stopping stream\n");
    err = LJM_eStreamStop(handle);
    check_error(err, "Stopping stream");
}
