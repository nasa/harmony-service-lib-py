import argparse
import harmony
import traceback

class ExampleAdapter(harmony.BaseHarmonyAdapter):
    # For running outside of Harmony.  Services should not generally override this method
    def completed_with_error(self, error_message):
        print("Overrode ExampleAdapter#completed_with_error.")
        print("  Would have sent an error Harmony with message: " + error_message)
        self.is_complete = True

    # For running outside of Harmony.  Services should not generally override this method
    def completed_with_local_file(self, filename):
        print("Overrode ExampleAdapter#completed_with_local_file.")
        print("  Would have staged " + filename + " and told Harmony to redirect to its URL")
        with open(filename) as f:
            print("  Output file contents: " + f.read())
        self.is_complete = True

    def invoke(self):
        # Get the data being requested.  Each granule will have a "filename" attribute indicating its local filename
        self.download_granules()

        # Build a single output file
        output_filename = 'tmp/harmony/output.txt'
        output_file = open(output_filename, 'w')
        for granule in self.message.granules:
            try:
                # Do work for each granule.  Usually this would involve calling a real service by transforming values
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

        # Stage the output file to a network-accessible location (signed S3 URL) and call back to Harmony
        # with that location.
        self.completed_with_local_file(output_filename)

        # Avoiding local temp file cleanup is useful in debugging and performing it is unnecessary
        # when running in docker.  Services should typically opt not to call this.
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
