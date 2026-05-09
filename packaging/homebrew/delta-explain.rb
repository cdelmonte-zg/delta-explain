class DeltaExplain < Formula
  desc "Make Delta pruning visible: CLI for partition pruning and data skipping in Delta Lake"
  homepage "https://github.com/cdelmonte-zg/delta-explain"
  version "0.2.1"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/cdelmonte-zg/delta-explain/releases/download/v0.2.1/delta-explain-aarch64-apple-darwin.tar.gz"
      sha256 "REPLACE_WITH_SHA256_AFTER_FIRST_RELEASE"
    end
    on_intel do
      url "https://github.com/cdelmonte-zg/delta-explain/releases/download/v0.2.1/delta-explain-x86_64-apple-darwin.tar.gz"
      sha256 "REPLACE_WITH_SHA256_AFTER_FIRST_RELEASE"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/cdelmonte-zg/delta-explain/releases/download/v0.2.1/delta-explain-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "REPLACE_WITH_SHA256_AFTER_FIRST_RELEASE"
    end
    on_intel do
      url "https://github.com/cdelmonte-zg/delta-explain/releases/download/v0.2.1/delta-explain-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "REPLACE_WITH_SHA256_AFTER_FIRST_RELEASE"
    end
  end

  def install
    bin.install "delta-explain"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/delta-explain --version")
  end
end
