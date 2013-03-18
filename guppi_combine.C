/* guppi_combine.C
 *
 * GUPPI-specific file combiner for fold-mode data
 */

#include "Pulsar/Application.h"
#include "Pulsar/StandardOptions.h"

#include "Pulsar/FrequencyAppend.h"
#include "Pulsar/PatchTime.h"

#include "Pulsar/Archive.h"
#include "Pulsar/Integration.h"
#include "Pulsar/FITSHdrExtension.h"

#include "Pulsar/Parameters.h"
#include "Pulsar/Predictor.h"

#include "dirutil.h"

#include <iostream>

using namespace std;
using namespace Pulsar;

//! GUPPI combiner application
class guppi_combine: public Pulsar::Application
{
    public:

        // Constructor
        guppi_combine();

        // Called before run.  Can be used to check cmd line
        // args for consistency etc.
        void setup();

        // The main part of the program.  We'll replace the
        // standard "loop over input files" behavior of the 
        // Application class.
        void run();

        // Called after run
        //void finalize();

        // We don't use this one here..
        void process (Archive *arch) { } 

    protected:

        //! Add extra command line options
        void add_options (CommandLine::Menu&);

        //! Name of output file
        string unload_name;

        //! Name of output dir
        string unload_dir;

        //! Data are in gpuN/ subdir structure under the given base
        string base_dir;

        //! Download data from gpu cluster
        bool transfer;

        //! List of nodes
        vector<string> cluster_nodes;

        //! Do the file transfer
        void do_transfer();
};

int main (int argc, char** argv)
{
    guppi_combine program;
    return program.main (argc, argv);
}

guppi_combine::guppi_combine() : Pulsar::Application("guppi_combine", 
        "combines fold-mode coherent GUPPI files from gpu cluster")
{
    version = "1.0";

    has_manual = false;
    update_history = false;
    sort_filenames = false;

    stow_script = false;

    cluster_nodes.clear();
    cluster_nodes.push_back("gpu1");
    cluster_nodes.push_back("gpu2");
    cluster_nodes.push_back("gpu3");
    cluster_nodes.push_back("gpu4");
    cluster_nodes.push_back("gpu5");
    cluster_nodes.push_back("gpu6");
    cluster_nodes.push_back("gpu7");
    cluster_nodes.push_back("gpu8");
    cluster_nodes.push_back("gpu9");

    add( new Pulsar::StandardOptions );
}

void guppi_combine::add_options (CommandLine::Menu& menu)
{
    CommandLine::Argument* arg;

    menu.add("\n" "General options:");

    arg = menu.add (unload_name, 'o', "fname");
    arg->set_help ("output results to 'fname'");

    arg = menu.add (unload_dir, 'O', "dir");
    arg->set_help ("output results to 'dir'");

    arg = menu.add (base_dir, 'D', "dir");
    arg->set_help ("use gpuN/ subdir structure under given base dir");
    // This is a bit of a hack, but will cause the first argument
    // given to be stored in the 'script' variable rather than
    // parsed as a filename if the -D option is given.  
    // This is necessary because when using -D the filename given 
    // on the command line does not exist in the current dir and
    // this will cause the program to quit during the parse() function.
    arg->set_notification(stow_script);
    arg->set_long_help(
            "This argument causes the program to look for data files of the \n"
            "specified name in the (dir)/gpu*/ subdirs.  By default the\n"
            "output is to (dir)/(filename), but that can be overridden using\n"
            "the -o option.  Only one filename should be given as an\n"
            "argument to the program when this option is in use."
            );

    arg = menu.add (transfer, 'T');
    arg->set_help("transfer data directly from gpu cluster nodes");
    // Same hack with 'script' variable happens in this case
    arg->set_notification(stow_script);
    arg->set_long_help(
            "With this option, the data will be temporarily stored under\n"
            "the directory name specified by -D, defaulting to /dev/shm.\n"
            "Files are deleted after they are combined.  For now, node\n"
            "names are hardcoded in this program."
            );

}

void guppi_combine::setup ()
{

    // Do file transfer as necessary
    if (transfer)
    {
        if (base_dir.empty())
            base_dir = "/dev/shm";
        do_transfer();
    }

    // If using dir structure, get list of files
    if (!base_dir.empty())
    {
        if (script.empty())
            throw Error (InvalidState, "guppi_combine::setup",
                    "No input filename given");
        if (filenames.size())
            throw Error (InvalidState, "guppi_combine::setup",
                    "When using -D option, only one input filename is allowed");
        if (unload_dir.empty())
            unload_dir = base_dir;
        if (unload_name.empty()) 
            unload_name = script;
        string filepattern;
        filepattern = base_dir + "/gpu*/" + script;
        filenames.clear();
        dirglob (&filenames, filepattern);
    }

    if (filenames.empty()) 
        throw Error(InvalidState, "guppi_combine::setup",
                "No input files given");

    // Accept at most 8 files
    const int max_files = 8;
    if (filenames.size() > max_files) 
        throw Error (InvalidState, "guppi_combine::setup",
                "maximum number of input files exceeded");

    // Need an output name
    if (unload_name.empty())
        throw Error (InvalidState, "guppi_combine::setup", 
                "specify output filename using -o");

    // Make full output name
    if (!unload_dir.empty())
        unload_name = unload_dir + "/" + unload_name;
}

void guppi_combine::do_transfer()
{
    // TODO: look into making this loop parallel to speed up transfer
    // of large files.
    for (unsigned inode=0; inode<cluster_nodes.size(); inode++)
    {
        string rfile = cluster_nodes[inode] + "-10::data/gpu/partial/" + 
            cluster_nodes[inode] + "/" + script;
        string dest = base_dir + "/" + cluster_nodes[inode] + "/";
        string cmd = "rsync -a " + rfile + " " + dest;
        if (verbose)
            cerr << name << ": execing '" << cmd << "'" << endl;
        system(cmd.c_str()); // check return?
    }
}

bool freq_compare(const Reference::To<Archive>& a1, 
        const Reference::To<Archive>& a2)
{
    return a1->get_centre_frequency() < a2->get_centre_frequency();
}

void guppi_combine::run ()
{

    // Load all the files
    vector< Reference::To<Archive> > archives;
    for (unsigned ifile=0; ifile<filenames.size(); ifile++) 
        archives.push_back( load(filenames[ifile]) );

    // Sort by freq
    sort(archives.begin(), archives.end(), freq_compare);

#if 0 
    // Figure out appropriate numbers of subints
    int min_nsub=-1, max_nsub=-1;
    for (unsigned iarch=0; iarch<archives.size(); iarch++)
    {
        int nsub = archives[iarch]->get_nsubint();
        if (min_nsub<0 || nsub<min_nsub) min_nsub = nsub;
        if (max_nsub<0 || nsub>max_nsub) max_nsub = nsub;
    }
#endif

    // Or, try the patch method...
    PatchTime patch;
    patch.set_contemporaneity_policy( new Contemporaneity::AtEarth );

    // Start with the mid-band archive
    Reference::To<Archive> out_arch = archives[archives.size() / 2];
    if (verbose)
        cerr << name << ": using '" << out_arch->get_filename()
            << "' as reference" << endl;
    FrequencyAppend freqappend;
    freqappend.init(out_arch);
    freqappend.ignore_phase = true; // we do our own rephasing 

    // Correct the FITS header stuff, assumes 8 nodes
    FITSHdrExtension *hdr = out_arch->get<FITSHdrExtension>();
    double freq_sum = hdr->obsfreq + hdr->obsbw/(double)hdr->obsnchan/2.0;
    hdr->obsbw *= 8.0;
    hdr->obsnchan *= 8;

    // Loop over all the others
    for (unsigned iarch=0; iarch<archives.size(); iarch++)
    {
        Reference::To<Archive> arch = archives[iarch];

        // Skip the reference archive
        if (arch == out_arch) continue;

        if (verbose)
            cerr << name << ": processing '" << arch->get_filename() 
                << "'" << endl;

        // How picky should we be about the input files matching up?
        // psrchive routines usually catch this kind of stuff..

        // Apply the ref profile polycos to the input files
        arch->set_model( out_arch->get_model() );

        // Fix missing subints
        patch.operate(out_arch, arch);

        // Append
        freqappend.append(out_arch, arch);

        // Get a sort-of-accurate original center freq
        hdr = arch->get<FITSHdrExtension>();
        freq_sum += hdr->obsfreq + hdr->obsbw/(double)hdr->obsnchan/2.0;

        // Delete the temp file if we are running in transfer mode
        if (transfer)
        {
            if (verbose)
                cerr << name << ": removing '" << arch->get_filename()
                    << "'" << endl;
            unlink(arch->get_filename().c_str()); // Check return
        }

        // deallocate when done (saving some memory)
        arch = NULL;
        archives[iarch] = NULL;

    }

    // Fix the center freq
    hdr = out_arch->get<FITSHdrExtension>();
    hdr->obsfreq = freq_sum / (double)archives.size();

    // Store original out_arch files
    string orig_fname = out_arch->get_filename();

    // All done, unload the thing
    if (verbose)
        cerr << name << ": unloading '" << unload_name << "'" << endl;
    out_arch->unload(unload_name);

    // Remove the last file if necessary
    if (transfer)
    {
        if (verbose)
            cerr << name << ":  removing '" << orig_fname << "'" << endl;
        unlink(orig_fname.c_str());
    }
        
}
