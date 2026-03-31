"""Test runner and reporting script for InsightFlow AI."""

from __future__ import annotations

import subprocess
import sys
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any


class TestRunner:
    """Run all tests and generate comprehensive report."""

    def __init__(self, backend_dir: Path):
        self.backend_dir = backend_dir
        self.test_results: Dict[str, Any] = {
            'timestamp': datetime.now().isoformat(),
            'tests': {},
            'summary': {},
            'recommendations': []
        }

    def run_test_suite(self, test_file: str, test_name: str) -> Dict[str, Any]:
        """Run a test suite and collect results."""
        print(f"\n{'='*70}")
        print(f"Running: {test_name}")
        print(f"{'='*70}")
        
        cmd = [
            sys.executable, '-m', 'pytest',
            f'tests/{test_file}',
            '-v', '--tb=short', '--json-report', '--json-report-file=/tmp/report.json'
        ]
        
        start_time = time.time()
        result = subprocess.run(cmd, cwd=self.backend_dir, capture_output=False)
        execution_time = time.time() - start_time
        
        return {
            'name': test_name,
            'file': test_file,
            'exit_code': result.returncode,
            'passed': result.returncode == 0,
            'execution_time': execution_time
        }

    def run_all_tests(self) -> None:
        """Run all test suites."""
        test_suites = [
            ('test_auth.py', 'Authentication Tests'),
            ('test_forecasting_service.py', 'Forecasting Service Tests'),
            ('test_endpoints_comprehensive.py', 'API Endpoints Tests'),
            ('test_performance.py', 'Performance Tests'),
        ]
        
        print("\n" + "="*70)
        print("InsightFlow AI - Comprehensive Test Suite")
        print("="*70)
        print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        for test_file, test_name in test_suites:
            result = self.run_test_suite(test_file, test_name)
            self.test_results['tests'][test_name] = result
        
        self.generate_summary()
        self.print_report()
        self.export_report()

    def generate_summary(self) -> None:
        """Generate test summary."""
        tests = self.test_results['tests']
        
        total_tests = len(tests)
        passed_tests = sum(1 for t in tests.values() if t['passed'])
        failed_tests = total_tests - passed_tests
        total_time = sum(t['execution_time'] for t in tests.values())
        
        self.test_results['summary'] = {
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'pass_rate': f"{(passed_tests/total_tests)*100:.1f}%" if total_tests > 0 else "0%",
            'total_execution_time': f"{total_time:.2f}s",
            'average_time_per_test': f"{(total_time/total_tests):.2f}s" if total_tests > 0 else "0s"
        }
        
        # Generate recommendations
        self._generate_recommendations(passed_tests, failed_tests)

    def _generate_recommendations(self, passed: int, failed: int) -> None:
        """Generate recommendations based on test results."""
        recommendations = []
        
        if failed > 0:
            recommendations.append(
                "⚠️ Fix failing tests - Review error logs and implement corrections"
            )
        
        if passed == 0:
            recommendations.append(
                "🔴 Critical: No tests passed. Check environment setup and dependencies"
            )
        elif passed > 0 and failed == 0:
            recommendations.append(
                "✅ All tests passed! Application is ready for deployment"
            )
        
        recommendations.append(
            "📊 Monitor performance metrics during production deployment"
        )
        recommendations.append(
            "🔄 Re-run tests after any code changes or dependency updates"
        )
        
        self.test_results['recommendations'] = recommendations

    def print_report(self) -> None:
        """Print test report to console."""
        summary = self.test_results['summary']
        
        print("\n" + "="*70)
        print("TEST SUMMARY")
        print("="*70)
        print(f"Total Test Suites:      {summary['total_tests']}")
        print(f"Passed:                 {summary['passed_tests']}")
        print(f"Failed:                 {summary['failed_tests']}")
        print(f"Pass Rate:              {summary['pass_rate']}")
        print(f"Total Execution Time:   {summary['total_execution_time']}")
        print(f"Average Time/Suite:     {summary['average_time_per_test']}")
        
        print("\n" + "="*70)
        print("TEST DETAILS")
        print("="*70)
        for test_name, result in self.test_results['tests'].items():
            status = "✅ PASSED" if result['passed'] else "❌ FAILED"
            print(f"{status} - {test_name} ({result['execution_time']:.2f}s)")
        
        print("\n" + "="*70)
        print("RECOMMENDATIONS")
        print("="*70)
        for i, rec in enumerate(self.test_results['recommendations'], 1):
            print(f"{i}. {rec}")

    def export_report(self) -> None:
        """Export test report to JSON."""
        report_path = self.backend_dir / 'test_report.json'
        
        with open(report_path, 'w') as f:
            json.dump(self.test_results, f, indent=2)
        
        print(f"\n✅ Test report exported to: {report_path}")


def run_tests_with_coverage() -> None:
    """Run tests with coverage analysis."""
    print("\n" + "="*70)
    print("Running Tests with Coverage Analysis")
    print("="*70)
    
    cmd = [
        sys.executable, '-m', 'pytest',
        'tests/',
        '--cov=app',
        '--cov-report=html',
        '--cov-report=term-missing',
        '-v'
    ]
    
    subprocess.run(cmd)


def validate_dependencies() -> bool:
    """Validate that all required dependencies are installed."""
    print("\nValidating dependencies...")
    
    required_packages = [
        'pytest',
        'pytest-asyncio',
        'httpx',
        'sqlalchemy',
        'pandas',
        'numpy',
        'fastapi',
        'uvicorn'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package}")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n⚠️ Missing packages: {', '.join(missing_packages)}")
        print(f"Install with: pip install {' '.join(missing_packages)}")
        return False
    
    return True


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Run comprehensive tests for InsightFlow AI'
    )
    parser.add_argument(
        '--coverage',
        action='store_true',
        help='Run tests with coverage analysis'
    )
    parser.add_argument(
        '--quick',
        action='store_true',
        help='Run quick tests only (skip performance tests)'
    )
    
    args = parser.parse_args()
    
    backend_dir = Path(__file__).parent.parent
    
    # Validate dependencies
    if not validate_dependencies():
        sys.exit(1)
    
    # Run tests
    runner = TestRunner(backend_dir)
    runner.run_all_tests()
    
    # Run coverage if requested
    if args.coverage:
        run_tests_with_coverage()
    
    # Exit with appropriate code
    summary = runner.test_results['summary']
    if summary['failed_tests'] > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
