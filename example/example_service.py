"""
==================
example_service.py
==================

An example service adapter implementation and example CLI parser
"""

import argparse
import harmony

from tempfile import mkstemp
from os import environ

# IMPORTANT: The following line avoids making real calls to a non-existent
# Harmony frontend.  Service authors should not set this variable to "dev"
# or "test" when releasing the service.
environ['ENV'] = 'dev'

class ExampleAdapter(harmony.BaseHarmonyAdapter):
    """
    Shows an example of what a service adapter implementation looks like
    """

    def invoke(self):
        """
        Example service adapter `#invoke` implementation.  Fetches data, reads granule info,
        puts the result in a file, calls back to Harmony, and cleans up after itself
        """
        # 1. Get the data being requested.  Each granule will have a "filename" attribute indicating its local filename
        self.download_granules()

        # 2. Build a single output file
        (flags, output_filename) = mkstemp(suffix='.txt', text=True)
        self.temp_paths += [output_filename] # Add it to the list of things to clean up
        output_file = open(output_filename, 'w')
        for granule in self.message.granules:
            try:
                # 3. Do work for each granule.  Usually this would involve calling a real service by transforming values
                # from the Harmony message
                print("Processing granule with ID %s from collection %s with variable(s): %s" %
                    (granule.id, granule.collection, ', '.join([v.id for v in granule.variables])))

                # Get the result into the single output file
                with open(granule.local_filename) as file:
                    output_file.write(file.read() + " ")
            except Exception as e:
                # Handle any specific errors, returning a useful message
                self.completed_with_error('Error processing granule: ' + granule.id)
                raise # re-raise, because we can't callback twice

        output_file.close()

        if self.message.isSynchronous:
            # 4(a). For requests that produce a single file where a user is holding open a connection waiting a result,
            #       Stage the output file to a network-accessible location (signed S3 URL) and call back to Harmony
            #       with that location.
            self.completed_with_local_file(output_filename)
        else:
            # 4(b). For requests that produce a multiple results or where a user is not waiting for a response,
            #       Stage the output file to a network-accessible location (signed S3 URL) and call back to Harmony
            #       with that location, including additional reference information on the result and operation
            #       Services can and should do this as individual results are produced and update the progress indicator
            #       ...
            self.async_add_local_file_partial_result(output_filename,
                is_variable_subset=True,
                is_regridded=False,
                is_subsetted=True,
                title='Example data',
                mime='text/plain',
                progress=50,
                temporal=harmony.Temporal(start='2020-01-01T00:00:00Z', end='2020-02-01T00:00:00Z'),
                bbox=[-100, -40, 100, 40])

            # 4(b). (Cont) Then call back once all files have been produced
            self.async_completed_successfully()

        # 5. Remove temporary files produced during execution
        self.cleanup()

def run_cli(args):
    """
    Runs the CLI.  Presently stubbed to demonstrate how a non-Harmony CLI fits in and allow
    future implementation or removal if desired.

    Parameters
    ----------
    args : Namespace
        Argument values parsed from the command line, presumably via ArgumentParser.parse_args

    Returns
    -------
    None
    """
    print("TODO: You can implement a non-Harmony CLI here.")
    print('To see the Harmony CLI, pass `--harmony-action=invoke --harmony-input="$(cat example/example_message.json)"`')

def main():
    """
    Parses command line arguments and invokes the appropriate method to respond to them

    Returns
    -------
    None
    """
    parser = argparse.ArgumentParser(prog='example', description='Run an example service')

    harmony.setup_cli(parser)

    args = parser.parse_args()

    if (harmony.is_harmony_cli(args)):
        harmony.run_cli(parser, args, ExampleAdapter)
    else:
        run_cli(args)

if __name__ == "__main__":
    main()
