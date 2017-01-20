package reactor.test;

/**
 * Options for a {@link StepVerifier}, including the initial request amount and
 * toggling of some checks.
 *
 * @author Simon Baslé
 */
public class StepVerifierOptions {

	private boolean checkUnderRequesting = true;
	private long initialRequest = Long.MAX_VALUE;

	/**
	 * Activate or deactivate the {@link StepVerifier} check of request amount
	 * being too low. Defauts to true.
	 *
	 * @param enabled true if the check should be enabled.
	 * @return this instance, to continue setting the options.
	 */
	public StepVerifierOptions checkUnderRequesting(boolean enabled) {
		this.checkUnderRequesting = enabled;
		return this;
	}

	/**
	 * @return true if the {@link StepVerifier} receiving these options should activate
	 * the check of request amount being too low.
	 */
	public boolean isCheckUnderRequesting() {
		return this.checkUnderRequesting;
	}

	/**
	 * Set the amount the {@link StepVerifier} should request initially. Defaults to
	 * unbounded request ({@code Long.MAX_VALUE}).
	 *
	 * @param initialRequest the initial request amount.
	 * @return this instance, to continue setting the options.
	 */
	public StepVerifierOptions initialRequest(long initialRequest) {
		this.initialRequest = initialRequest;
		return this;
	}

	/**
	 * @return the initial request amount to be made by the {@link StepVerifier}
	 * receiving these options.
	 */
	public long getInitialRequest() {
		return this.initialRequest;
	}

}
